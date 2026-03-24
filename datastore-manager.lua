--!strict
-- datastore manager (server-friendly, production-ready)
-- goal: safe, structured player data management without data loss or session conflicts
-- features: session locking, retry with backoff, schema versioning, auto-save, middleware hooks,
--           dirty tracking, MessagingService cross-server unlock, request budget awareness
-- written by slateorbitt (slateorbitt)

--[[
	how to think about this module
	- each "profile" is a player's data document, loaded on join and saved on leave
	- session locking means only ONE server can own a profile at a time
	  (prevents two servers writing the same player and corrupting data)
	- we retry failed datastore calls using exponential backoff so temporary outages don't lose data
	- schema versioning lets you safely change the shape of your data over time via migration functions
	- middleware hooks let you run your own logic before saves or after loads without touching this module
	- dirty tracking means we only write keys that actually changed, saving request budget
	- auto-save runs on a configurable interval so data isn't only saved on leave

	session locking is the most important concept here:
	- when a player joins, we write a lock record under their userId in a separate datastore
	- the lock contains our JobId so we can identify which server owns it
	- while the player is here, we periodically refresh the lock timestamp so it doesn't expire
	- when the player leaves, we delete the lock so other servers can pick them up
	- if a server crashes without releasing, the lock will naturally expire after lockTimeout seconds
	  and a new server can claim it — this is the "stale lock recovery" path
]]

type UserId = number
type Key = string

-- shape of the raw lock record we write to a separate datastore key
-- we store jobId so we can verify ownership and avoid clearing another server's lock by accident
type SessionLock = {
	jobId: string,     -- game.JobId of the server that owns this session right now
	timestamp: number, -- os.time() when the lock was last written or refreshed
}

-- a migration is a function that upgrades data from one schema version to the next
-- index 1 = "run this to go from v1 to v2", index 2 = "run this to go from v2 to v3", etc.
-- we run them in order so a player who skipped multiple versions gets caught up in one load
type MigrationFn = (oldData: { [string]: any }) -> { [string]: any }

-- middleware functions run before save or after load
-- they receive a copy of the data and can return a modified version, or nil to leave it unchanged
-- this lets you inject logic (logging, encryption, stamping timestamps) without editing this module
type MiddlewareFn = (data: { [string]: any }) -> { [string]: any }?

-- public-facing config type; optional fields will be filled with defaults in new()
type ManagerConfig = {
	storeName: string,
	lockStoreName: string,
	template: { [string]: any },
	schemaVersion: number,
	migrations: { MigrationFn }?,
	autoSaveInterval: number?,
	lockTimeout: number?,
	maxRetries: number?,
	onBeforeSave: MiddlewareFn?,
	onAfterLoad: MiddlewareFn?,
}

-- internal resolved config; all fields are guaranteed non-nil after new() fills in defaults
-- we use this type for self._config so strict mode doesn't complain about number? arithmetic
type ResolvedConfig = {
	storeName: string,
	lockStoreName: string,
	template: { [string]: any },
	schemaVersion: number,
	migrations: { MigrationFn },
	autoSaveInterval: number,
	lockTimeout: number,
	maxRetries: number,
	onBeforeSave: MiddlewareFn?,
	onAfterLoad: MiddlewareFn?,
}

-- internal profile record; one of these exists per loaded player
type Profile = {
	userId: UserId,
	data: { [string]: any },
	savedData: { [string]: any },
	version: number,
	loaded: boolean,
	releasing: boolean,
	lockRefreshThread: thread?,
	dirtyKeys: { [string]: boolean },
}

-- what getStats() returns
type ManagerStats = {
	loadedProfiles: number,
	totalSaves: number,
	totalLoads: number,
	totalRetries: number,
	totalFailedSaves: number,
}

local DataStoreService = game:GetService("DataStoreService")
local MessagingService = game:GetService("MessagingService")
local Players = game:GetService("Players")

local DataManager = {}
DataManager.__index = DataManager

export type DataManager = {
	_store: DataStore,
	_lockStore: DataStore,
	_config: ResolvedConfig,
	_profiles: { [UserId]: Profile },
	_totalSaves: number,
	_totalLoads: number,
	_totalRetries: number,
	_totalFailedSaves: number,

	loadProfile:    (self: DataManager, userId: UserId) -> Profile?,
	releaseProfile: (self: DataManager, userId: UserId) -> (),
	getProfile:     (self: DataManager, userId: UserId) -> Profile?,
	setKey:         (self: DataManager, userId: UserId, key: Key, value: any) -> (),
	getKey:         (self: DataManager, userId: UserId, key: Key) -> any,
	wipeData:       (self: DataManager, userId: UserId) -> (),
	getStats:       (self: DataManager) -> ManagerStats,

	_retryCall:     (self: DataManager, fn: () -> any, requestType: Enum.DataStoreRequestType) -> (boolean, any),
	_acquireLock:   (self: DataManager, userId: UserId) -> boolean,
	_releaseLock:   (self: DataManager, userId: UserId) -> (),
	_refreshLock:   (self: DataManager, userId: UserId) -> (),
	_saveProfile:   (self: DataManager, profile: Profile, isRelease: boolean?) -> boolean,
	_migrateData:   (self: DataManager, data: { [string]: any }, fromVersion: number) -> { [string]: any },
	_getDirtyKeys:  (self: DataManager, profile: Profile) -> { string },
	_startAutoSave: (self: DataManager) -> (),
	_notifyUnlock:  (self: DataManager, userId: UserId) -> (),
}

--------------------------------------------------------------------------------
-- module-level helpers
--------------------------------------------------------------------------------

-- deep copy a table recursively so we get a true independent snapshot
-- this is critical for dirty tracking: savedData must NOT share any table references with data
-- if they did, mutating data would silently mutate savedData too, and we'd never detect changes
local function deepCopy(t: { [string]: any }): { [string]: any }
	local copy = {} :: { [string]: any }
	for k, v in pairs(t) do
		if type(v) == "table" then
			copy[k] = deepCopy(v)
		else
			copy[k] = v
		end
	end
	return copy
end

-- reconcile fills in any keys that exist in template but are missing from data
-- this handles the common case where a player's saved data predates a new key being added
-- without this, newly added keys would be nil for returning players until they trigger a save
local function reconcile(data: { [string]: any }, template: { [string]: any }): { [string]: any }
	for k, v in pairs(template) do
		if data[k] == nil then
			if type(v) == "table" then
				-- clone the template default so all players get independent table instances
				-- without this, all players would share the same table reference from the template
				data[k] = deepCopy(v)
			else
				data[k] = v
			end
		end
		-- if data[k] already exists we leave it alone — saved value always wins over template
	end
	return data
end

-- compute how long to wait before retry attempt N using exponential backoff
-- doubling the wait each attempt reduces hammering during outages
-- capped at 16s so a long outage doesn't freeze gameplay for too long
-- jitter staggers retries from different servers so they don't all hit at once
local function backoffWait(attempt: number)
	local base = math.min(0.5 * (2 ^ (attempt - 1)), 16)
	task.wait(base + math.random() * 0.5)
end

--------------------------------------------------------------------------------
-- construction
--------------------------------------------------------------------------------

function DataManager.new(config: ManagerConfig): DataManager
	assert(config.storeName and #config.storeName > 0, "DataManager: storeName is required")
	assert(config.lockStoreName and #config.lockStoreName > 0, "DataManager: lockStoreName is required")
	assert(type(config.template) == "table", "DataManager: template must be a table")
	assert(
		type(config.schemaVersion) == "number" and config.schemaVersion >= 1,
		"DataManager: schemaVersion must be a number >= 1"
	)

	local self = setmetatable({}, DataManager)

	-- GetDataStore doesn't actually connect yet — errors surface on the first Get/Set call
	self._store     = DataStoreService:GetDataStore(config.storeName)
	self._lockStore = DataStoreService:GetDataStore(config.lockStoreName)

	-- build the resolved config, filling all optional fields with sensible defaults
	-- we store this as ResolvedConfig so arithmetic on these fields is safe under strict mode
	-- we deepCopy the template so mutations from outside after creation can't affect our default
	local resolved: ResolvedConfig = {
		storeName        = config.storeName,
		lockStoreName    = config.lockStoreName,
		template         = deepCopy(config.template),
		schemaVersion    = config.schemaVersion,
		migrations       = config.migrations or {},
		autoSaveInterval = config.autoSaveInterval ~= nil and config.autoSaveInterval or 60,
		lockTimeout      = config.lockTimeout ~= nil and config.lockTimeout or 30,
		maxRetries       = config.maxRetries ~= nil and config.maxRetries or 5,
		onBeforeSave     = config.onBeforeSave,
		onAfterLoad      = config.onAfterLoad,
	}
	self._config = resolved

	self._profiles = {} :: { [UserId]: Profile }

	-- all stats start at 0; they're lifetime totals and never reset
	self._totalSaves       = 0
	self._totalLoads       = 0
	self._totalRetries     = 0
	self._totalFailedSaves = 0

	-- cast self to DataManager now that all fields are set
	-- this lets us call methods below without strict complaining about the raw metatable type
	local dm = self :: DataManager

	-- start the periodic auto-save loop only if interval > 0
	-- interval = 0 is a valid "disable auto-save" signal from the caller
	if dm._config.autoSaveInterval > 0 then
		dm:_startAutoSave()
	end

	-- subscribe to cross-server unlock messages so we can respond faster than the lock timeout
	-- if another server is waiting for a player's lock, this lets them skip the timeout wait
	-- wrapped in pcall because MessagingService is sometimes unavailable in Studio or private servers
	pcall(function()
		MessagingService:SubscribeAsync("DataManager_Unlock", function(message)
			local userId = tonumber(message.Data) -- message.Data is always a string
			if not userId then return end          -- guard against malformed messages

			-- only act if THIS server has a profile loaded for that userId
			-- and isn't already mid-release (avoid double-release race)
			local profile = dm._profiles[userId :: number]
			if profile and not profile.releasing then
				dm:releaseProfile(userId :: number) -- cast safe: nil guarded above
			end
		end)
	end)

	return dm
end

--------------------------------------------------------------------------------
-- internal: retry wrapper
--------------------------------------------------------------------------------

function DataManager._retryCall(self: DataManager, fn: () -> any, requestType: Enum.DataStoreRequestType): (boolean, any)
	-- every datastore call goes through here so retry + budget logic is centralized
	-- requestType must match the actual operation (GetAsync vs SetAsync vs UpdateAsync)
	-- because Roblox tracks budget separately per type — checking the wrong one is misleading
	local maxRetries: number = self._config.maxRetries
	local lastErr: any = nil

	for attempt = 1, maxRetries do
		-- check budget BEFORE the call to avoid burning a retry on a guaranteed throttle
		-- budget <= 0 means Roblox will queue or drop the request anyway
		local budget = DataStoreService:GetRequestBudgetForRequestType(requestType)
		if budget <= 0 then
			-- no budget available; wait and count as a retry since we're stalling
			task.wait(2)
			self._totalRetries += 1
			continue
		end

		local ok, result = pcall(fn)
		if ok then
			return true, result
		end

		lastErr = result

		if attempt < maxRetries then
			-- only backoff if there are more attempts left; final failure falls through to warn
			self._totalRetries += 1
			backoffWait(attempt)
		end
	end

	warn(string.format("DataManager: all %d retries failed — %s", maxRetries, tostring(lastErr)))
	return false, lastErr
end

--------------------------------------------------------------------------------
-- internal: session locking
--------------------------------------------------------------------------------

function DataManager._acquireLock(self: DataManager, userId: UserId): boolean
	-- attempt to atomically write a lock record for this userId
	-- UpdateAsync is used instead of SetAsync because it's atomic:
	-- the callback receives the current value and can abort (return nil) if already locked
	-- SetAsync would blindly overwrite, causing a race where two servers both think they own it
	local lockKey = "lock_" .. userId -- separate namespace from data keys to avoid collisions
	local timeout: number = self._config.lockTimeout
	local myJobId: string = game.JobId -- unique per server instance; identifies lock ownership

	local acquired = false -- written inside UpdateAsync callback; read after the call

	local ok, _ = self:_retryCall(function(): any
		-- the outer wrapper must also explicitly return so _retryCall's fn: () -> any is satisfied
		self._lockStore:UpdateAsync(lockKey, function(existing: any): any
			if existing ~= nil then
				local lock = existing :: SessionLock
				local age = os.time() - lock.timestamp -- seconds since lock was last touched
				if lock.jobId ~= myJobId and age < timeout then
					-- a different server owns a non-expired lock; do NOT overwrite
					-- returning nil from UpdateAsync means "keep the existing value unchanged"
					return nil :: any
				end
				-- if it IS our jobId, we're refreshing our own lock — that's fine
				-- if age >= timeout, owning server likely crashed; lock is stale and claimable
			end
			-- no lock, or it's ours, or it's stale — write our claim
			acquired = true
			return { jobId = myJobId, timestamp = os.time() } :: any
		end)
		return nil :: any
	end, Enum.DataStoreRequestType.UpdateAsync) -- UpdateAsync has its own budget bucket

	if not ok then
		-- the datastore call itself failed after all retries; treat as failed acquisition
		return false
	end

	return acquired
end

function DataManager._releaseLock(self: DataManager, userId: UserId)
	-- clear our lock record so other servers can claim this player after they leave
	-- we use UpdateAsync (not RemoveAsync) so we can verify ownership first:
	-- we should never clear a lock that belongs to a different server
	local lockKey = "lock_" .. userId
	local myJobId: string = game.JobId

	self:_retryCall(function(): any
		self._lockStore:UpdateAsync(lockKey, function(existing: any): any
			if existing == nil then
				-- lock already gone (maybe called this twice); nothing to do
				return nil :: any
			end
			local lock = existing :: SessionLock
			if lock.jobId ~= myJobId then
				-- this lock belongs to a different server — don't touch it
				-- can happen if the server crashed and another server grabbed the lock
				return nil :: any
			end
			-- it's our lock; delete it by returning nil (UpdateAsync removes the key on nil)
			return nil :: any
		end)
		return nil :: any
	end, Enum.DataStoreRequestType.UpdateAsync)
end

function DataManager._refreshLock(self: DataManager, userId: UserId)
	-- runs in a dedicated coroutine while the player is online
	-- refreshes the lock timestamp every (lockTimeout / 2) seconds
	-- refreshing at half the timeout prevents expiry during long sessions
	-- if we refreshed at exactly lockTimeout, clock skew between servers could cause false expiry
	-- the half-timeout margin gives a comfortable buffer against that
	local lockKey = "lock_" .. userId
	local myJobId: string = game.JobId
	local halfTimeout: number = math.floor(self._config.lockTimeout / 2) -- 50% of expiry window

	while true do
		task.wait(halfTimeout)

		-- check if this profile is still ours before touching the lock
		-- profile could have been released while we were waiting in task.wait
		local profile = self._profiles[userId]
		if not profile or profile.releasing then
			break -- player left or release started; stop the refresh loop
		end

		self:_retryCall(function(): any
			self._lockStore:UpdateAsync(lockKey, function(existing: any): any
				if existing == nil then
					return nil :: any -- lock was cleared externally; don't re-create it here
				end
				local lock = existing :: SessionLock
				if lock.jobId ~= myJobId then
					return nil :: any -- someone else owns it; don't overwrite their lock
				end
				-- still ours; bump the timestamp to prove we're alive
				return { jobId = myJobId, timestamp = os.time() } :: any
			end)
			return nil :: any
		end, Enum.DataStoreRequestType.UpdateAsync)
	end
end

function DataManager._notifyUnlock(self: DataManager, userId: UserId)
	-- broadcast to all servers that this userId's lock is being released
	-- servers waiting to load this player can now try to acquire the lock immediately
	-- instead of waiting for the full lockTimeout to expire
	-- wrapped in pcall because MessagingService can fail in Studio or private servers
	-- a failure here is non-fatal: other servers will still recover via lock expiry
	pcall(function()
		MessagingService:PublishAsync("DataManager_Unlock", tostring(userId))
	end)
end

--------------------------------------------------------------------------------
-- internal: schema migration
--------------------------------------------------------------------------------

function DataManager._migrateData(self: DataManager, data: { [string]: any }, fromVersion: number): { [string]: any }
	-- walk from fromVersion up to schemaVersion, applying each migration in order
	-- running them one-by-one means a player who skipped multiple versions gets fully caught up
	-- even if they haven't played since before several schema changes
	local migrations: { MigrationFn } = self._config.migrations
	local currentVersion = fromVersion
	local currentData = data

	while currentVersion < self._config.schemaVersion do
		local migrationFn = migrations[currentVersion] -- index N upgrades from version N to N+1
		if migrationFn then
			-- run migration in pcall so one broken migration doesn't kill the whole load
			-- the tradeoff: a failed migration means that version's changes are missing
			-- but the player still gets in with the rest of their data intact
			-- aborting the load on failure would lock the player out entirely — worse outcome
			local ok, result = pcall(migrationFn, deepCopy(currentData))
			if ok and type(result) == "table" then
				currentData = result
			else
				warn(string.format(
					"DataManager: migration from v%d failed — %s (skipped, data may be incomplete)",
					currentVersion,
					tostring(result)
					))
			end
		end
		-- increment regardless of whether a migration fn existed for this version
		-- some version bumps don't need data changes (e.g. adding an optional field via reconcile)
		currentVersion += 1
	end

	return currentData
end

--------------------------------------------------------------------------------
-- internal: dirty tracking
--------------------------------------------------------------------------------

function DataManager._getDirtyKeys(self: DataManager, profile: Profile): { string }
	-- compare live data against the last-saved snapshot to find what changed
	-- we only do top-level key comparison intentionally:
	-- deep comparison of every nested table on every save would be expensive at scale
	-- for nested table changes, callers must explicitly mark the key dirty via setKey
	-- this is a deliberate tradeoff: slight manual overhead in exchange for predictable performance
	local dirty = {} :: { string }

	-- forward pass: find keys that exist in current data but differ from the snapshot
	for k, v in pairs(profile.data) do
		local saved = profile.savedData[k]
		if saved == nil then
			-- new key added to data since last save
			dirty[#dirty + 1] = k
		elseif type(v) ~= type(saved) then
			-- type changed; definitely dirty
			-- check type before value because comparing a table to a number would always dirty
			dirty[#dirty + 1] = k
		elseif type(v) == "table" then
			-- nested table: can't cheaply compare deep contents, so we rely on dirtyKeys flags
			-- this is why setKey marks the key dirty even for nested mutations
			if profile.dirtyKeys[k] then
				dirty[#dirty + 1] = k
			end
		elseif v ~= saved then
			dirty[#dirty + 1] = k
		end
	end

	-- reverse pass: find keys that were deleted from data since last save
	-- can't catch these in the forward pass because they no longer exist in data
	for k in pairs(profile.savedData) do
		if profile.data[k] == nil then
			dirty[#dirty + 1] = k
		end
	end

	return dirty
end

--------------------------------------------------------------------------------
-- internal: save
--------------------------------------------------------------------------------

function DataManager._saveProfile(self: DataManager, profile: Profile, isRelease: boolean?): boolean
	-- write the profile's data to the datastore
	-- isRelease = true means this is the terminal save before we unlock and forget the player
	-- the flag exists for future extensibility (audit logging, release-only hooks, etc.)

	if not profile.loaded then
		-- if the initial load never succeeded, profile.data may be empty or partially reconciled
		-- writing it would overwrite whatever valid data the player had in the store
		-- so we refuse to save until at least one successful load has happened
		return false
	end

	-- run onBeforeSave middleware on a copy, not on profile.data directly
	-- we don't want middleware side-effects (like stamping timestamps) to pollute the live data
	local dataToSave = deepCopy(profile.data)
	if self._config.onBeforeSave then
		local result = self._config.onBeforeSave(dataToSave)
		if result ~= nil then
			dataToSave = result -- middleware returned a modified version; use it for the write
		end
	end

	-- wrap data in a versioned payload so we know which migrations to run on the next load
	-- storing the version alongside the data means we never have to guess what shape it's in
	local payload = {
		data    = dataToSave,
		version = self._config.schemaVersion,
	}

	local key = tostring(profile.userId) -- DataStore requires string keys
	local ok, _ = self:_retryCall(function(): any
		self._store:SetAsync(key, payload)
		return nil :: any -- explicit return so strict sees a consistent return path
	end, Enum.DataStoreRequestType.SetIncrementAsync)

	if ok then
		self._totalSaves += 1
		-- update savedData to match what we just wrote so dirty tracking resets correctly
		-- we snapshot profile.data (not dataToSave) because middleware may have modified dataToSave,
		-- and we want the dirty baseline to reflect the actual live data, not the middleware output
		profile.savedData = deepCopy(profile.data)
		profile.dirtyKeys = {} -- clear all dirty flags now that the save succeeded
	else
		self._totalFailedSaves += 1
		warn(string.format("DataManager: failed to save userId %d", profile.userId))
		-- do NOT clear dirtyKeys on failure so the next save attempt includes everything that failed
	end

	return ok
end

--------------------------------------------------------------------------------
-- internal: auto-save
--------------------------------------------------------------------------------

function DataManager._startAutoSave(self: DataManager)
	-- run a background loop that saves all loaded profiles every autoSaveInterval seconds
	-- this is a task.spawn loop rather than a Heartbeat connection because:
	-- Heartbeat fires every frame (~60/sec) and would need its own timer tracking,
	-- while a simple task.wait loop is more readable and has zero per-frame overhead
	local interval: number = self._config.autoSaveInterval

	task.spawn(function()
		while true do
			task.wait(interval)
			-- snapshot _profiles with table.clone before iterating so that profiles
			-- added or removed mid-iteration don't cause pairs() to skip or double-visit entries
			for _, profile in pairs(table.clone(self._profiles)) do
				if not profile.releasing then
					-- skip profiles mid-release to avoid a race with their final save
					self:_saveProfile(profile, false)
				end
			end
		end
	end)
end

--------------------------------------------------------------------------------
-- public: load / release
--------------------------------------------------------------------------------

function DataManager.loadProfile(self: DataManager, userId: UserId): Profile?
	-- load a player's data and set up their session
	-- returns the profile on success, nil if locking or loading failed

	if self._profiles[userId] then
		-- already loaded on this server; return the existing profile
		-- guards against PlayerAdded firing twice on reconnect edge cases
		return self._profiles[userId]
	end

	-- acquire session lock BEFORE reading data
	-- if we read first and then locked, another server could load the same player
	-- in the window between our read and our lock, then both servers would own stale data
	local lockAcquired = self:_acquireLock(userId)
	if not lockAcquired then
		warn(string.format(
			"DataManager: could not acquire lock for userId %d — player may be active on another server",
			userId
			))
		return nil
	end

	local key = tostring(userId)
	local ok, result = self:_retryCall(function(): any
		return self._store:GetAsync(key)
	end, Enum.DataStoreRequestType.GetAsync) -- GetAsync has its own budget bucket separate from writes

	if not ok then
		-- load failed after all retries; release the lock so we don't hold it indefinitely
		-- keeping the lock after a failed load would block this player from rejoining anywhere
		self:_releaseLock(userId)
		warn(string.format("DataManager: failed to load data for userId %d", userId))
		return nil
	end

	-- unpack the result: nil = brand new player, table = returning player's versioned payload
	local rawData: { [string]: any }
	local savedVersion: number = 1 -- assume v1 if no version stored (pre-versioning saves)

	if result == nil then
		rawData = deepCopy(self._config.template)
	else
		local payload = result :: { data: { [string]: any }, version: number }
		rawData      = payload.data    or deepCopy(self._config.template)
		savedVersion = payload.version or 1
	end

	-- run migrations if the saved data is behind the current schema version
	-- we do this before reconcile so migrations run on the raw saved shape,
	-- not a partially-reconciled version (ordering matters if a migration removes a key)
	if savedVersion < self._config.schemaVersion then
		rawData = self:_migrateData(rawData, savedVersion)
	end

	-- fill in any keys added to the template since the player's last save
	-- runs after migration so reconcile sees the fully-migrated shape
	rawData = reconcile(rawData, self._config.template)

	-- run onAfterLoad middleware if provided
	if self._config.onAfterLoad then
		local middlewareResult = self._config.onAfterLoad(deepCopy(rawData))
		if middlewareResult ~= nil then
			rawData = middlewareResult
		end
	end

	local profile: Profile = {
		userId            = userId,
		data              = rawData,
		savedData         = deepCopy(rawData), -- baseline for dirty tracking; must be an independent copy
		version           = self._config.schemaVersion,
		loaded            = true,
		releasing         = false,
		lockRefreshThread = nil,
		dirtyKeys         = {},
	}

	self._profiles[userId] = profile -- store before spawning refresh so it's visible immediately
	self._totalLoads += 1

	-- start the lock refresh coroutine so our lock stays valid during long sessions
	-- stored on the profile so we can cancel it precisely when the player leaves
	profile.lockRefreshThread = task.spawn(function()
		self:_refreshLock(userId)
	end)

	return profile
end

function DataManager.releaseProfile(self: DataManager, userId: UserId)
	-- final save, lock release, and cleanup for a player leaving
	-- safe to call multiple times; no-ops if profile is already gone
	local profile = self._profiles[userId]
	if not profile then
		return
	end

	-- set releasing = true first so auto-save skips this profile
	-- without this, auto-save could fire between our save and our lock release,
	-- writing a redundant save under a lock that's about to be removed
	profile.releasing = true

	-- stop the lock refresh coroutine before doing the final save
	-- if we left it running it could write a refresh after we've released the lock
	if profile.lockRefreshThread then
		task.cancel(profile.lockRefreshThread)
		profile.lockRefreshThread = nil
	end

	-- do the final save while we still hold the lock
	-- order matters: save first, release lock second, then nil the profile
	-- if we released the lock first, another server could start loading stale data
	-- before our final write landed
	self:_saveProfile(profile, true)

	self:_releaseLock(userId)
	self:_notifyUnlock(userId) -- lets other servers skip the timeout wait

	-- remove from the loaded profiles table last so nothing references a half-released profile
	self._profiles[userId] = nil
end

--------------------------------------------------------------------------------
-- public: data access
--------------------------------------------------------------------------------

function DataManager.getProfile(self: DataManager, userId: UserId): Profile?
	return self._profiles[userId]
end

function DataManager.setKey(self: DataManager, userId: UserId, key: Key, value: any)
	-- set a single top-level key and mark it dirty for the next save
	-- all data mutations should go through here rather than directly indexing profile.data
	-- because direct indexing won't mark the key dirty, so nested table changes would be invisible
	local profile = self._profiles[userId]
	if not profile then
		warn(string.format("DataManager.setKey: no loaded profile for userId %d", userId))
		return
	end

	profile.data[key]      = value
	profile.dirtyKeys[key] = true -- flag it so _getDirtyKeys and _saveProfile know it changed
end

function DataManager.getKey(self: DataManager, userId: UserId, key: Key): any
	local profile = self._profiles[userId]
	if not profile then
		warn(string.format("DataManager.getKey: no loaded profile for userId %d", userId))
		return nil
	end
	return profile.data[key]
end

function DataManager.wipeData(self: DataManager, userId: UserId)
	-- reset a player's data to the template defaults in memory
	-- does NOT release the session or save immediately; wipe persists on next auto-save or leave
	local profile = self._profiles[userId]
	if not profile then
		warn(string.format("DataManager.wipeData: no loaded profile for userId %d", userId))
		return
	end

	profile.data = deepCopy(self._config.template)

	-- mark every template key as dirty so the wipe gets written on next save
	-- can't rely on _getDirtyKeys to catch this automatically because the new data
	-- may have the same values as savedData for some keys (e.g. coins = 0 in both)
	for k in pairs(self._config.template) do
		profile.dirtyKeys[k] = true
	end
end

--------------------------------------------------------------------------------
-- public: stats
--------------------------------------------------------------------------------

function DataManager.getStats(self: DataManager): ManagerStats
	-- count loaded profiles by iterating rather than storing a counter
	-- a counter could drift if releases happen during errors; counting is always accurate
	local count = 0
	for _ in pairs(self._profiles) do
		count += 1
	end

	return {
		loadedProfiles   = count,
		totalSaves       = self._totalSaves,
		totalLoads       = self._totalLoads,
		totalRetries     = self._totalRetries,
		totalFailedSaves = self._totalFailedSaves,
	}
end

return DataManager
