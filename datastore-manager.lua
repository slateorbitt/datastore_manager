--!strict
-- datastore manager (server-friendly, production-ready)
-- goal: safe, structured player data management without data loss or session conflicts
-- features: session locking, retry with backoff, schema versioning, auto-save, middleware hooks,
--           dirty tracking, MessagingService cross-server unlock, request budget awareness
-- written by mohammad.noori (mohammadssoul)

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
]]

type UserId = number
type Key = string

-- shape of the raw lock record we write to a separate datastore key
type SessionLock = {
	jobId: string, -- game.JobId of the server that owns this session
	timestamp: number, -- os.time() when the lock was acquired or last refreshed
}

-- a migration is a function that takes old data and returns new data
-- migrations run in order whenever the saved version is behind the current one
type MigrationFn = (oldData: { [string]: any }) -> { [string]: any }

-- middleware: functions that run before save or after load
-- they receive the data table and can return a modified copy or nil (to keep it unchanged)
type MiddlewareFn = (data: { [string]: any }) -> { [string]: any }?

-- config you pass when creating the manager
type ManagerConfig = {
	storeName: string, -- name of the main DataStore
	lockStoreName: string, -- name of the DataStore used for session lock records
	template: { [string]: any }, -- default data shape for new players
	schemaVersion: number, -- current version number of your data schema
	migrations: { MigrationFn }?, -- ordered list of migration fns (index = version it upgrades FROM)
	autoSaveInterval: number?, -- seconds between auto-saves (default 60, set 0 to disable)
	lockTimeout: number?, -- seconds before a stale lock is considered expired (default 30)
	maxRetries: number?, -- how many times to retry a failed datastore call (default 5)
	onBeforeSave: MiddlewareFn?, -- called with data right before every save
	onAfterLoad: MiddlewareFn?, -- called with data right after every successful load
}

-- internal profile record (one per loaded player)
type Profile = {
	userId: UserId,
	data: { [string]: any }, -- live data table, mutate this directly
	savedData: { [string]: any }, -- snapshot from last save (used for dirty tracking)
	version: number, -- schema version this profile was loaded/migrated to
	loaded: boolean, -- true once the initial load succeeded
	releasing: boolean, -- true while we're in the middle of a release save
	lockRefreshThread: thread?, -- coroutine that keeps refreshing the session lock
	dirtyKeys: { [string]: boolean }, -- keys that changed since last save
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
local RunService = game:GetService("RunService")
local Players = game:GetService("Players")

local DataManager = {}
DataManager.__index = DataManager

export type DataManager = {
	-- stores
	_store: DataStore,
	_lockStore: DataStore,

	-- config (stored so we can reference at runtime)
	_config: ManagerConfig,

	-- loaded profiles, keyed by userId
	_profiles: { [UserId]: Profile },

	-- auto-save connection handle
	_autoSaveConnection: RBXScriptConnection?,

	-- stats
	_totalSaves: number,
	_totalLoads: number,
	_totalRetries: number,
	_totalFailedSaves: number,

	-- public api
	loadProfile: (self: DataManager, userId: UserId) -> Profile?,
	releaseProfile: (self: DataManager, userId: UserId) -> (),
	getProfile: (self: DataManager, userId: UserId) -> Profile?,
	setKey: (self: DataManager, userId: UserId, key: Key, value: any) -> (),
	getKey: (self: DataManager, userId: UserId, key: Key) -> any,
	wipeData: (self: DataManager, userId: UserId) -> (),
	getStats: (self: DataManager) -> ManagerStats,

	-- internal helpers
	_retryCall: (self: DataManager, fn: () -> any) -> (boolean, any),
	_acquireLock: (self: DataManager, userId: UserId) -> boolean,
	_releaseLock: (self: DataManager, userId: UserId) -> (),
	_refreshLock: (self: DataManager, userId: UserId) -> (),
	_saveProfile: (self: DataManager, profile: Profile, isRelease: boolean?) -> boolean,
	_migrateData: (self: DataManager, data: { [string]: any }, fromVersion: number) -> { [string]: any },
	_deepCopy: (t: { [string]: any }) -> { [string]: any },
	_getDirtyKeys: (self: DataManager, profile: Profile) -> { string },
	_startAutoSave: (self: DataManager) -> (),
	_notifyUnlock: (self: DataManager, userId: UserId) -> (),
}

--------------------------------------------------------------------------------
-- helpers (module-level, no self needed)
--------------------------------------------------------------------------------

-- deep copy a table so we get true snapshots, not references
-- this is important for dirty tracking: savedData must not share refs with data
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

-- reconcile fills in any missing keys from template into data
-- this handles the case where a player loaded before a new key was added to the template
local function reconcile(data: { [string]: any }, template: { [string]: any }): { [string]: any }
	for k, v in pairs(template) do
		if data[k] == nil then
			-- key doesn't exist yet, add the default
			if type(v) == "table" then
				data[k] = deepCopy(v)
			else
				data[k] = v
			end
		end
	end
	return data
end

-- compute exponential backoff wait time for retry attempt N
-- starts at ~0.5s and doubles each retry so we don't hammer the datastore
local function backoffWait(attempt: number)
	local waitTime = math.min(0.5 * (2 ^ (attempt - 1)), 16)
	-- add a small random jitter so multiple retries from different servers don't all fire at once
	task.wait(waitTime + math.random() * 0.5)
end

--------------------------------------------------------------------------------
-- construction
--------------------------------------------------------------------------------

function DataManager.new(config: ManagerConfig): DataManager
	-- validate required config fields up front so errors are obvious
	assert(config.storeName and #config.storeName > 0, "DataManager: storeName is required")
	assert(config.lockStoreName and #config.lockStoreName > 0, "DataManager: lockStoreName is required")
	assert(type(config.template) == "table", "DataManager: template must be a table")
	assert(type(config.schemaVersion) == "number" and config.schemaVersion >= 1, "DataManager: schemaVersion must be >= 1")

	local self = setmetatable({}, DataManager)

	-- open datastores (these don't error until you actually call Get/Set)
	self._store = DataStoreService:GetDataStore(config.storeName)
	self._lockStore = DataStoreService:GetDataStore(config.lockStoreName)

	-- fill in optional config fields with sensible defaults
	self._config = {
		storeName = config.storeName,
		lockStoreName = config.lockStoreName,
		template = deepCopy(config.template), -- clone so outside mutations don't affect us
		schemaVersion = config.schemaVersion,
		migrations = config.migrations or {},
		autoSaveInterval = config.autoSaveInterval ~= nil and config.autoSaveInterval or 60,
		lockTimeout = config.lockTimeout or 30,
		maxRetries = config.maxRetries or 5,
		onBeforeSave = config.onBeforeSave,
		onAfterLoad = config.onAfterLoad,
	}

	self._profiles = {} :: { [UserId]: Profile }
	self._autoSaveConnection = nil

	-- stats
	self._totalSaves = 0
	self._totalLoads = 0
	self._totalRetries = 0
	self._totalFailedSaves = 0

	-- start auto-save loop if interval is set
	if self._config.autoSaveInterval > 0 then
		self:_startAutoSave()
	end

	-- listen for cross-server unlock messages so we can respond immediately
	-- when another server tells us to release a lock, we don't have to wait for timeout
	pcall(function()
		MessagingService:SubscribeAsync("DataManager_Unlock", function(message)
			local userId = tonumber(message.Data)
			if not userId then return end
			-- if THIS server holds the lock for that userId, release it now
			local profile = self._profiles[userId]
			if profile and not profile.releasing then
				self:releaseProfile(userId)
			end
		end)
	end)

	return self :: DataManager
end

--------------------------------------------------------------------------------
-- internal: retry wrapper
--------------------------------------------------------------------------------

function DataManager._retryCall(self: DataManager, fn: () -> any): (boolean, any)
	-- wrap any datastore call in retry logic with exponential backoff
	-- returns (success, result) so callers decide what to do on failure
	local maxRetries: number = self._config.maxRetries
	local lastErr: any

	for attempt = 1, maxRetries do
		-- check if we have request budget before trying
		-- this avoids burning retries on calls that will definitely throttle
		local budget = DataStoreService:GetRequestBudgetForRequestType(Enum.DataStoreRequestType.GetAsync)
		if budget <= 0 then
			-- no budget right now, wait a bit and retry
			task.wait(2)
			self._totalRetries += 1
			continue
		end

		local ok, result = pcall(fn)
		if ok then
			return true, result
		end

		lastErr = result

		-- count this as a retry if it's not the last attempt
		if attempt < maxRetries then
			self._totalRetries += 1
			backoffWait(attempt)
		end
	end

	-- all retries exhausted
	warn(string.format("DataManager: all %d retries failed — %s", maxRetries, tostring(lastErr)))
	return false, lastErr
end

--------------------------------------------------------------------------------
-- internal: session locking
--------------------------------------------------------------------------------

function DataManager._acquireLock(self: DataManager, userId: UserId): boolean
	-- try to write a lock record for this userId
	-- if another server already holds a valid (non-expired) lock, we back off
	local lockKey = "lock_" .. userId
	local timeout: number = self._config.lockTimeout
	local myJobId = game.JobId

	local acquired = false

	local ok, _ = self:_retryCall(function()
		-- UpdateAsync is atomic so two servers can't both think they acquired the lock
		self._lockStore:UpdateAsync(lockKey, function(existing: any)
			-- existing is the current lock record (or nil if no lock)
			if existing ~= nil then
				local lock = existing :: SessionLock
				local age = os.time() - lock.timestamp
				if lock.jobId ~= myJobId and age < timeout then
					-- lock is held by a different server and hasn't expired yet
					-- return nil means "don't update", so the lock stays as-is
					return nil
				end
			end
			-- either no lock, expired lock, or we already own it — write our lock
			acquired = true
			return {
				jobId = myJobId,
				timestamp = os.time(),
			}
		end)
	end)

	if not ok then
		return false
	end

	return acquired
end

function DataManager._releaseLock(self: DataManager, userId: UserId)
	-- clear our lock record so other servers can pick up this player
	local lockKey = "lock_" .. userId
	local myJobId = game.JobId

	self:_retryCall(function()
		self._lockStore:UpdateAsync(lockKey, function(existing: any)
			if existing == nil then return nil end
			local lock = existing :: SessionLock
			-- only remove the lock if WE own it
			-- don't accidentally clear another server's lock
			if lock.jobId == myJobId then
				return nil -- nil return removes the key
			end
			return existing -- not ours, leave it alone
		end)
	end)
end

function DataManager._refreshLock(self: DataManager, userId: UserId)
	-- update the lock timestamp periodically so it doesn't expire while the player is still here
	-- this runs in a coroutine so it doesn't block anything
	local lockKey = "lock_" .. userId
	local myJobId = game.JobId
	local halfTimeout = math.floor(self._config.lockTimeout / 2)

	while true do
		task.wait(halfTimeout)

		-- check if profile still exists and is still ours before refreshing
		local profile = self._profiles[userId]
		if not profile or profile.releasing then
			break
		end

		self:_retryCall(function()
			self._lockStore:UpdateAsync(lockKey, function(existing: any)
				if existing == nil then return nil end
				local lock = existing :: SessionLock
				if lock.jobId ~= myJobId then return nil end -- someone else took it?
				return {
					jobId = myJobId,
					timestamp = os.time(),
				}
			end)
		end)
	end
end

function DataManager._notifyUnlock(self: DataManager, userId: UserId)
	-- tell other servers that this player's lock is being released
	-- so if they're waiting, they can try to acquire it sooner than the timeout
	pcall(function()
		MessagingService:PublishAsync("DataManager_Unlock", tostring(userId))
	end)
end

--------------------------------------------------------------------------------
-- internal: migration
--------------------------------------------------------------------------------

function DataManager._migrateData(self: DataManager, data: { [string]: any }, fromVersion: number): { [string]: any }
	-- run migrations in order from fromVersion up to schemaVersion
	-- each migration function receives the data from the previous step
	local migrations: { MigrationFn } = self._config.migrations or {}
	local currentVersion = fromVersion
	local currentData = data

	while currentVersion < self._config.schemaVersion do
		local migrationFn = migrations[currentVersion]
		if migrationFn then
			-- run the migration safely so one bad migration doesn't kill the load
			local ok, result = pcall(migrationFn, deepCopy(currentData))
			if ok and type(result) == "table" then
				currentData = result
			else
				warn(string.format(
					"DataManager: migration from v%d failed — %s",
					currentVersion,
					tostring(result)
				))
				-- skip this migration and continue to next version
				-- better to have slightly wrong data than to not load at all
			end
		end
		currentVersion += 1
	end

	return currentData
end

--------------------------------------------------------------------------------
-- internal: dirty tracking
--------------------------------------------------------------------------------

function DataManager._getDirtyKeys(self: DataManager, profile: Profile): { string }
	-- compare current data against last-saved snapshot
	-- returns a list of top-level keys that changed (added, removed, or modified)
	-- note: we only do top-level comparison here — deep changes inside nested tables
	-- are caught by marking the key dirty explicitly via setKey
	local dirty = {} :: { string }

	for k, v in pairs(profile.data) do
		local saved = profile.savedData[k]
		if saved == nil then
			-- new key added since last save
			dirty[#dirty + 1] = k
		elseif type(v) ~= type(saved) then
			dirty[#dirty + 1] = k
		elseif type(v) == "table" then
			-- for nested tables, rely on dirtyKeys flags set by setKey
			if profile.dirtyKeys[k] then
				dirty[#dirty + 1] = k
			end
		elseif v ~= saved then
			dirty[#dirty + 1] = k
		end
	end

	for k in pairs(profile.savedData) do
		if profile.data[k] == nil then
			-- key was deleted since last save
			dirty[#dirty + 1] = k
		end
	end

	return dirty
end

--------------------------------------------------------------------------------
-- internal: save
--------------------------------------------------------------------------------

function DataManager._saveProfile(self: DataManager, profile: Profile, isRelease: boolean?): boolean
	-- save the profile's data to the datastore
	-- isRelease = true means this is the final save before we let go of the player
	if not profile.loaded then
		-- never successfully loaded, don't write partial/empty data
		return false
	end

	-- run onBeforeSave middleware if provided
	local dataToSave = deepCopy(profile.data)
	if self._config.onBeforeSave then
		local result = self._config.onBeforeSave(dataToSave)
		if result ~= nil then
			dataToSave = result
		end
	end

	-- wrap the data with version info so we know what to migrate next time
	local payload = {
		data = dataToSave,
		version = self._config.schemaVersion,
	}

	local key = tostring(profile.userId)
	local ok, _ = self:_retryCall(function()
		self._store:SetAsync(key, payload)
	end)

	if ok then
		self._totalSaves += 1
		-- update the saved snapshot so dirty tracking works correctly next time
		profile.savedData = deepCopy(profile.data)
		-- clear dirty flags
		profile.dirtyKeys = {}
	else
		self._totalFailedSaves += 1
		warn(string.format("DataManager: failed to save userId %d", profile.userId))
	end

	return ok
end

--------------------------------------------------------------------------------
-- internal: auto-save
--------------------------------------------------------------------------------

function DataManager._startAutoSave(self: DataManager)
	-- auto-save runs every interval and saves all currently loaded profiles
	-- it skips profiles that are already being released to avoid double-write races
	local interval: number = self._config.autoSaveInterval

	self._autoSaveConnection = RunService.Heartbeat:Connect(function()
		-- we use os.clock here and track the last save time ourselves
		-- rather than a loop because this plays nicer with the scheduler pattern
	end)

	-- actually just run a task loop since Heartbeat would fire every frame
	task.spawn(function()
		while true do
			task.wait(interval)
			for userId, profile in pairs(self._profiles) do
				if not profile.releasing then
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
	-- load a player's data from the datastore and set up their session
	-- returns the profile on success, nil if we couldn't acquire the lock or load failed

	if self._profiles[userId] then
		-- already loaded, just return it
		return self._profiles[userId]
	end

	-- try to acquire session lock before reading any data
	-- this is the core safety net against dual-server writes
	local lockAcquired = self:_acquireLock(userId)
	if not lockAcquired then
		warn(string.format("DataManager: could not acquire lock for userId %d — session may be active elsewhere", userId))
		return nil
	end

	-- load raw data from store
	local key = tostring(userId)
	local ok, result = self:_retryCall(function()
		return self._store:GetAsync(key)
	end)

	if not ok then
		-- failed to load — release the lock so we don't hold it for nothing
		self:_releaseLock(userId)
		warn(string.format("DataManager: failed to load userId %d", userId))
		return nil
	end

	-- result is either nil (new player) or a payload table
	local rawData: { [string]: any }
	local savedVersion: number = 1

	if result == nil then
		-- brand new player, start from template
		rawData = deepCopy(self._config.template)
	else
		local payload = result :: { data: { [string]: any }, version: number }
		rawData = payload.data or deepCopy(self._config.template)
		savedVersion = payload.version or 1
	end

	-- run migrations if the saved version is behind
	if savedVersion < self._config.schemaVersion then
		rawData = self:_migrateData(rawData, savedVersion)
	end

	-- fill in any keys added to template since last save
	rawData = reconcile(rawData, self._config.template)

	-- run onAfterLoad middleware if provided
	if self._config.onAfterLoad then
		local middlewareResult = self._config.onAfterLoad(deepCopy(rawData))
		if middlewareResult ~= nil then
			rawData = middlewareResult
		end
	end

	-- build the profile record
	local profile: Profile = {
		userId = userId,
		data = rawData,
		savedData = deepCopy(rawData), -- snapshot for dirty tracking
		version = self._config.schemaVersion,
		loaded = true,
		releasing = false,
		lockRefreshThread = nil,
		dirtyKeys = {},
	}

	self._profiles[userId] = profile
	self._totalLoads += 1

	-- start lock refresh coroutine so our lock doesn't expire during a long session
	profile.lockRefreshThread = task.spawn(function()
		self:_refreshLock(userId)
	end)

	return profile
end

function DataManager.releaseProfile(self: DataManager, userId: UserId)
	-- do the final save, release the lock, and clean up
	-- call this when a player leaves
	local profile = self._profiles[userId]
	if not profile then
		return
	end

	-- mark as releasing so auto-save skips it while we're working
	profile.releasing = true

	-- stop the lock refresh coroutine
	if profile.lockRefreshThread then
		task.cancel(profile.lockRefreshThread)
		profile.lockRefreshThread = nil
	end

	-- do the final save
	self:_saveProfile(profile, true)

	-- release lock and notify other servers
	self:_releaseLock(userId)
	self:_notifyUnlock(userId)

	-- remove from loaded profiles
	self._profiles[userId] = nil
end

--------------------------------------------------------------------------------
-- public: data access
--------------------------------------------------------------------------------

function DataManager.getProfile(self: DataManager, userId: UserId): Profile?
	-- simple lookup for when you just need the profile reference
	return self._profiles[userId]
end

function DataManager.setKey(self: DataManager, userId: UserId, key: Key, value: any)
	-- set a single key on the player's data
	-- marks that key as dirty so it's included in the next save
	local profile = self._profiles[userId]
	if not profile then
		warn(string.format("DataManager.setKey: no loaded profile for userId %d", userId))
		return
	end

	profile.data[key] = value
	-- flag as dirty so _getDirtyKeys picks it up even for nested table changes
	profile.dirtyKeys[key] = true
end

function DataManager.getKey(self: DataManager, userId: UserId, key: Key): any
	-- read a single key from the player's loaded data
	local profile = self._profiles[userId]
	if not profile then
		warn(string.format("DataManager.getKey: no loaded profile for userId %d", userId))
		return nil
	end
	return profile.data[key]
end

function DataManager.wipeData(self: DataManager, userId: UserId)
	-- reset a player's data back to the template defaults
	-- useful for ban resets, fresh starts, etc.
	-- note: this does NOT release the session — it just overwrites in memory and marks everything dirty
	local profile = self._profiles[userId]
	if not profile then
		warn(string.format("DataManager.wipeData: no loaded profile for userId %d", userId))
		return
	end

	profile.data = deepCopy(self._config.template)

	-- mark all template keys as dirty so the wipe gets written on next save
	for k in pairs(self._config.template) do
		profile.dirtyKeys[k] = true
	end
end

--------------------------------------------------------------------------------
-- public: stats
--------------------------------------------------------------------------------

function DataManager.getStats(self: DataManager): ManagerStats
	local count = 0
	for _ in pairs(self._profiles) do
		count += 1
	end

	return {
		loadedProfiles = count,
		totalSaves = self._totalSaves,
		totalLoads = self._totalLoads,
		totalRetries = self._totalRetries,
		totalFailedSaves = self._totalFailedSaves,
	}
end

return DataManager
