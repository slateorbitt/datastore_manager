--!strict
-- datastore manager (server-friendly, production-ready)
-- goal: safe, structured player data management without data loss or session conflicts
-- features: session locking, retry with backoff, schema versioning, auto-save, middleware hooks,
--           dirty tracking with change log, proxy data table, MessagingService cross-server unlock,
--           request budget awareness, per-key onChange callbacks
-- written by slbt (slateorbitt)

--[[
	how to think about this module
	- each "profile" is a player's data document, loaded on join and saved on leave
	- session locking means only ONE server can own a profile at a time
	  (prevents two servers writing the same player and corrupting data)
	- we retry failed datastore calls using exponential backoff so temporary outages don't lose data
	- schema versioning lets you safely change the shape of your data over time via migration functions
	- middleware hooks let you run logic before saves or after loads without touching this module
	- dirty tracking skips saves when nothing changed, saving request budget
	- profile.data is a proxy table backed by a metatable, so writing to it directly
	  (e.g. profile.data.coins += 10) automatically marks that key dirty and logs the change
	- a change log records every write since the last save so you can audit what changed
	- onKeyChanged fires immediately whenever any key is written through the proxy

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

-- a migration upgrades data from one schema version to the next
-- index N = "run this to go from version N to N+1"
-- we run them in order so a player who skipped multiple versions gets caught up in one load
type MigrationFn = (oldData: { [string]: any }) -> { [string]: any }

-- middleware functions run before save or after load
-- they receive a copy of the data and can return a modified version, or nil to leave it unchanged
-- this lets you inject logic (logging, encryption, stamping timestamps) without editing this module
type MiddlewareFn = (data: { [string]: any }) -> { [string]: any }?

-- a single entry in the change log; one is appended every time a key is written via the proxy
-- the log is cleared on every successful save so it only reflects changes since the last write
type ChangeEntry = {
	key: string,
	oldValue: any, -- value before the write (nil if the key didn't exist yet)
	newValue: any, -- value after the write
}

-- public-facing config type; optional fields will be filled with defaults in new()
type ManagerConfig = {
	storeName: string,
	lockStoreName: string,
	template: { [string]: any },
	schemaVersion: number,
	migrations: { MigrationFn }?,
	autoSaveInterval: number?,  -- seconds between auto-saves (default 60; 0 = disabled)
	lockTimeout: number?,       -- seconds before a stale lock is claimable (default 30)
	maxRetries: number?,        -- max retry attempts per datastore call (default 5)
	onBeforeSave: MiddlewareFn?,
	onAfterLoad: MiddlewareFn?,
	onKeyChanged: ((userId: UserId, key: string, oldValue: any, newValue: any) -> ())?, -- fires on every proxy write
}

-- internal resolved config; all fields guaranteed non-nil after new() fills in defaults
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
	onKeyChanged: ((userId: UserId, key: string, oldValue: any, newValue: any) -> ())?,
}

-- ring buffer for the change log
-- instead of a plain array with table.remove(log, 1) on overflow (O(n)),
-- we use a fixed-size circular buffer with a write head that wraps around
-- insertions are always O(1) regardless of cap size — the head just advances
-- when the buffer is full, the oldest entry is silently overwritten by the newest
-- this is the standard tradeoff for capped audit logs: bounded memory, O(1) writes,
-- slightly more complex iteration (but getChangelog handles that for callers)
type ChangeLog = {
	entries: { ChangeEntry? }, -- fixed-size array; slots may be nil before the buffer fills
	head: number,              -- index where the NEXT write goes (1-based, wraps at cap)
	count: number,             -- how many entries are currently stored (<= cap)
	cap: number,               -- maximum number of entries; set once at creation
}

-- internal profile record; one per loaded player
-- profile.data is a proxy table — write to it directly and dirty tracking handles the rest
type Profile = {
	userId: UserId,
	data: { [string]: any },      -- proxy table; __newindex auto-marks dirty and logs changes
	_rawData: { [string]: any },  -- the real underlying storage behind the proxy
	savedData: { [string]: any }, -- deep snapshot from last successful save; used to detect changes
	version: number,
	loaded: boolean,
	releasing: boolean,
	lockRefreshThread: thread?,
	dirtyKeys: { [string]: boolean },
	changelog: ChangeLog, -- ring buffer of writes since the last successful save
	_proxyCache: { [any]: any }, -- maps rawTable reference -> proxy; cleared on release to free memory
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
	getChangelog:   (self: DataManager, userId: UserId) -> { ChangeEntry },
	getStats:       (self: DataManager) -> ManagerStats,

	_retryCall:     (self: DataManager, fn: () -> any, requestType: Enum.DataStoreRequestType) -> (boolean, any),
	_acquireLock:   (self: DataManager, userId: UserId) -> boolean,
	_releaseLock:   (self: DataManager, userId: UserId) -> (),
	_refreshLock:   (self: DataManager, userId: UserId) -> (),
	_saveProfile:   (self: DataManager, profile: Profile, isRelease: boolean?) -> boolean,
	_migrateData:   (self: DataManager, data: { [string]: any }, fromVersion: number) -> { [string]: any },
	_getDirtyKeys:  (self: DataManager, profile: Profile) -> { string },
	_makeProxy:     (self: DataManager, profile: Profile) -> { [string]: any },
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
-- handles the case where a player's saved data predates a new key being added to the template
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
-- doubling the wait each attempt reduces hammering the datastore during outages
-- capped at 16s so a long outage doesn't freeze gameplay for too long
-- jitter staggers retries from different servers so they don't all hit simultaneously
local function backoffWait(attempt: number)
	local base = math.min(0.5 * (2 ^ (attempt - 1)), 16) -- 0.5 → 1 → 2 → 4 → 8 → 16 (capped)
	task.wait(base + math.random() * 0.5)                  -- add up to 0.5s of random jitter
end

-- create a new empty ring buffer with the given capacity
local function newChangeLog(cap: number): ChangeLog
	return {
		entries = table.create(cap) :: { ChangeEntry? },
		head    = 1,   -- starts at slot 1; advances on every insert
		count   = 0,   -- grows until it hits cap, then stays there
		cap     = cap,
	}
end

-- insert one entry into the ring buffer in O(1)
-- when full, the oldest entry is silently overwritten — we keep the newest cap entries
local function changeLogInsert(log: ChangeLog, entry: ChangeEntry)
	log.entries[log.head] = entry
	-- advance head, wrapping around when it reaches the end
	-- modulo arithmetic: (head - 1 + 1) % cap + 1 simplifies to head % cap + 1
	log.head = log.head % log.cap + 1
	-- count grows until it hits cap; after that it stays at cap (buffer is full)
	if log.count < log.cap then
		log.count += 1
	end
end

-- read all entries from the ring buffer in chronological order (oldest first)
-- when the buffer is full, oldest is at head (the slot about to be overwritten next)
-- when not full, entries are stored linearly from slot 1 to count
local function changeLogRead(log: ChangeLog): { ChangeEntry }
	local result = {} :: { ChangeEntry }
	if log.count < log.cap then
		-- buffer not yet full; entries are in slots 1..count in insertion order
		for i = 1, log.count do
			local e = log.entries[i]
			if e then result[#result + 1] = e end
		end
	else
		-- buffer full; oldest entry is at head (next write position), wrap around from there
		local idx = log.head
		for _ = 1, log.cap do
			local e = log.entries[idx]
			if e then result[#result + 1] = e end
			idx = idx % log.cap + 1
		end
	end
	return result
end

-- reset the ring buffer to empty without reallocating the backing array
-- called after every successful save so the log only reflects changes since the last write
local function changeLogClear(log: ChangeLog)
	table.clear(log.entries :: any)
	log.head  = 1
	log.count = 0
end

--------------------------------------------------------------------------------
-- construction
--------------------------------------------------------------------------------

function DataManager.new(config: ManagerConfig): DataManager
	-- validate required fields immediately so errors point here, not somewhere deep in the call stack
	assert(config.storeName and #config.storeName > 0, "DataManager: storeName is required")
	assert(config.lockStoreName and #config.lockStoreName > 0, "DataManager: lockStoreName is required")
	assert(config.storeName ~= config.lockStoreName,
		"DataManager: storeName and lockStoreName must be different — sharing a store corrupts lock records")
	assert(type(config.template) == "table", "DataManager: template must be a table")
	assert(
		type(config.schemaVersion) == "number" and config.schemaVersion >= 1,
		"DataManager: schemaVersion must be a number >= 1"
	)

	-- validate optional numeric fields if provided
	-- we check these even though they have defaults because a caller passing 0 or -1 accidentally
	-- would get silent bad behavior (infinite retry loops, negative waits) without these guards
	if config.autoSaveInterval ~= nil then
		assert(
			config.autoSaveInterval >= 0,
			"DataManager: autoSaveInterval must be >= 0 (0 = disabled)"
		)
		assert(
			config.autoSaveInterval <= 3600,
			"DataManager: autoSaveInterval must be <= 3600 — intervals over an hour risk data loss on crash"
		)
	end
	if config.lockTimeout ~= nil then
		assert(
			config.lockTimeout >= 5,
			"DataManager: lockTimeout must be >= 5 — values below 5s risk false expiry under normal latency"
		)
		assert(
			config.lockTimeout <= 300,
			"DataManager: lockTimeout must be <= 300 — very long timeouts delay recovery from server crashes"
		)
	end
	if config.maxRetries ~= nil then
		assert(
			config.maxRetries >= 1,
			"DataManager: maxRetries must be >= 1 — 0 retries means a single transient error loses data"
		)
		assert(
			config.maxRetries <= 10,
			"DataManager: maxRetries must be <= 10 — more than 10 retries can block threads for too long"
		)
	end

	local self = setmetatable({}, DataManager)

	-- GetDataStore doesn't actually connect yet — errors surface on the first Get/Set call
	self._store     = DataStoreService:GetDataStore(config.storeName)
	self._lockStore = DataStoreService:GetDataStore(config.lockStoreName)

	-- build the resolved config, filling all optional fields with sensible defaults
	-- stored as ResolvedConfig so all fields are non-optional and arithmetic is safe under strict
	-- deepCopy the template so mutations from outside after creation can't affect our internal default
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
		onKeyChanged     = config.onKeyChanged,
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
-- internal: proxy data table
--------------------------------------------------------------------------------

function DataManager._makeProxy(self: DataManager, profile: Profile): { [string]: any }
	-- build a proxy table that sits in front of profile._rawData
	-- the proxy uses __index and __newindex metamethods to intercept all reads and writes
	-- this is the core of the automatic dirty-tracking system:
	-- writing profile.data.coins = 10 marks "coins" dirty automatically
	--
	-- NESTED TABLE SAFETY:
	-- the naive approach only catches top-level writes like profile.data.coins = 10
	-- but NOT nested writes like profile.data.inventory[1] = "sword"
	-- because the nested write's __newindex fires on the inventory table, not on the root proxy
	-- our solution: whenever __index returns a table value, we wrap it in a nested proxy too
	-- the nested proxy's __newindex marks the PARENT top-level key dirty on any write
	-- so profile.data.inventory[1] = "sword" automatically marks "inventory" dirty
	-- this works recursively to any depth (profile.data.a.b.c = x marks "a" dirty)
	--
	-- PROXY CACHE:
	-- every __index call for a table would normally create a fresh proxy wrapper each time
	-- that's wasteful: profile.data.inventory[1] and profile.data.inventory[2] in the same
	-- frame would each allocate a new proxy wrapping the same underlying table
	-- instead we maintain a proxyCache keyed by rawTable reference so the same underlying
	-- table always returns the same proxy object — one allocation per nested table, ever
	--
	-- CHANGELOG CAP:
	-- without a cap, a mutation-heavy session with a long save interval could accumulate
	-- hundreds or thousands of log entries and grow memory unboundedly
	-- we keep the newest maxChangelogSize entries and discard the oldest when we go over
	-- the newest entries are the most actionable; the oldest ones are pre-empted by later writes

	local MAX_CHANGELOG = 200 -- ring buffer capacity; oldest entries overwritten when exceeded
	local userId = profile.userId

	-- cache maps rawTable reference -> proxy wrapper for that table
	-- stored on the profile (not as a local) so releaseProfile can explicitly clear it
	-- without this, released profiles would keep nested table references alive until GC
	-- with it, we guarantee cleanup at a known point: the moment the session ends
	local proxyCache = profile._proxyCache

	-- makeNestedProxy is declared as any to avoid the strict-mode union type that forms
	-- when you declare a typed forward reference and then assign the function to it
	-- the recursive call inside the function body would fail type resolution on the union
	-- declaring as any and casting at the call site is the standard Luau pattern for recursive locals
	local makeNestedProxy: any

	makeNestedProxy = function(rawTable: { [string]: any }, topLevelKey: string): { [string]: any }
		-- return cached proxy if we already wrapped this exact table
		-- identity comparison on the rawTable reference is what we want here:
		-- two different tables with the same contents are still different and need separate proxies
		local cached = proxyCache[rawTable]
		if cached then
			return cached
		end

		local nestedProxy = {}
		local nestedMeta = {}

		nestedMeta.__index = function(_p: any, k: string): any
			local v = rawTable[k]
			if type(v) == "table" then
				-- recurse: deeper table still marks the same topLevelKey dirty
				-- because the datastore saves whole top-level keys, not nested paths
				return makeNestedProxy(v, topLevelKey)
			end
			return v
		end

		nestedMeta.__newindex = function(_p: any, k: string, newValue: any)
			local oldNested = rawTable[k] -- capture old value before writing for accurate log

			rawTable[k] = newValue -- write to the actual nested table in _rawData

			-- mark the top-level key dirty — this is the unit the datastore cares about
			-- there is no partial-key save, so flagging "inventory" covers inventory[1], [2], etc.
			profile.dirtyKeys[topLevelKey] = true

			-- O(1) ring buffer insert; if the buffer is full, oldest entry is silently overwritten
			-- oldNested is the actual element value before this write, not nil — precise audit trail
			changeLogInsert(profile.changelog, {
				key      = topLevelKey,
				oldValue = oldNested,
				newValue = newValue,
			} :: ChangeEntry)

			local onKeyChanged = self._config.onKeyChanged
			if onKeyChanged then
				-- cast pcall to (any, ...any) -> (boolean, any) so strict sees 2 return values
				-- without this cast, strict infers return count from the callback's signature
				-- and since onKeyChanged returns nothing, it thinks pcall only returns 1 value
				local ok, err = (pcall :: (any, ...any) -> (boolean, any))(onKeyChanged, userId, topLevelKey, oldNested, newValue)
				if not ok then
					warn(string.format("DataManager: onKeyChanged error for key '%s' — %s", topLevelKey, tostring(err)))
				end
			end
		end

		nestedMeta.__len = function(_p: any): number
			-- cast to array type before # because rawTable is typed as { [string]: any }
			-- and strict warns about using # on string-keyed tables; the cast is safe here
			-- since __len is only meaningful when the caller treats it as a sequence
			return #(rawTable :: { any })
		end

		-- store in cache before returning so recursive calls can find it
		local built = setmetatable(nestedProxy, nestedMeta) :: any
		proxyCache[rawTable] = built
		return built
	end

	-- build the root proxy that sits directly in front of profile._rawData
	local proxy = {}
	local proxyMeta = {}

	proxyMeta.__index = function(_proxy: any, key: string): any
		local v = profile._rawData[key]
		if type(v) == "table" then
			-- wrap in nested proxy so writes inside are tracked; cached so no redundant allocations
			return makeNestedProxy(v, key)
		end
		return v
	end

	-- __newindex: all top-level writes are intercepted here
	proxyMeta.__newindex = function(_proxy: any, key: string, newValue: any)
		local oldValue = profile._rawData[key] -- capture before writing for accurate changelog

		profile._rawData[key] = newValue
		profile.dirtyKeys[key] = true

		-- if the new value is a table, invalidate its cache entry
		-- the old proxy (if any) was wrapping the old table reference; the new one needs a fresh proxy
		if type(newValue) == "table" then
			proxyCache[newValue] = nil
		end

		-- O(1) ring buffer insert; oldest entry silently overwritten when buffer is full
		changeLogInsert(profile.changelog, {
			key      = key,
			oldValue = oldValue,
			newValue = newValue,
		} :: ChangeEntry)

		local onKeyChanged = self._config.onKeyChanged
		if onKeyChanged then
			local ok, err = (pcall :: (any, ...any) -> (boolean, any))(onKeyChanged, userId, key, oldValue, newValue)
			if not ok then
				warn(string.format("DataManager: onKeyChanged error for key '%s' — %s", key, tostring(err)))
			end
		end
	end

	proxyMeta.__len = function(_proxy: any): number
		return #(profile._rawData :: { any }) -- cast: strict warns about # on string-keyed tables
	end

	return setmetatable(proxy, proxyMeta) :: any
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
			task.wait(2) -- short wait to let the budget partially recover before retrying
			self._totalRetries += 1
			continue -- skip pcall this attempt; budget would have rejected it
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
		self._lockStore:UpdateAsync(lockKey, function(existing: any): any
			if existing ~= nil then
				local lock = existing :: SessionLock
				local age = os.time() - lock.timestamp -- seconds since the lock was last touched
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
		return nil :: any -- outer wrapper must return to satisfy fn: () -> any
	end, Enum.DataStoreRequestType.UpdateAsync) -- UpdateAsync has its own budget bucket

	if not ok then
		return false
	end

	return acquired
end

function DataManager._releaseLock(self: DataManager, userId: UserId)
	-- release our session lock so other servers can claim this player after they leave
	-- IMPORTANT: UpdateAsync returning nil only cancels the write — it does NOT delete the key
	-- DataStore has no single atomic "delete if owner" operation, so we use a two-phase approach:
	--
	-- phase 1 (atomic): UpdateAsync to verify ownership and stamp timestamp = 0
	--   timestamp = 0 means lock age is instantly > any lockTimeout everywhere
	--   so other servers will treat it as stale and claimable even if phase 2 never completes
	--   this makes the invalidation atomic — the moment we write, the lock becomes harmless
	--
	-- phase 2 (best-effort): RemoveAsync to actually delete the key for cleanliness
	--   if this fails, the timestamp = 0 from phase 1 already did the important work
	--   a stale lock sitting in the store doesn't block anyone
	local lockKey = "lock_" .. userId
	local myJobId: string = game.JobId
	local wasOurs = false -- set in UpdateAsync; tells phase 2 whether to bother removing

	-- phase 1: atomic ownership check + invalidation
	self:_retryCall(function(): any
		self._lockStore:UpdateAsync(lockKey, function(existing: any): any
			if existing == nil then
				return nil :: any -- already gone; nothing to do
			end
			local lock = existing :: SessionLock
			if lock.jobId ~= myJobId then
				return nil :: any -- not ours; do not touch it
			end
			-- it's ours — stamp timestamp to 0 so any server can immediately claim it
			-- even if RemoveAsync below fails, this zero timestamp is the real safety net
			wasOurs = true
			return { jobId = myJobId, timestamp = 0 } :: any
		end)
		return nil :: any
	end, Enum.DataStoreRequestType.UpdateAsync)

	-- phase 2: best-effort cleanup; only bother if we confirmed ownership in phase 1
	if wasOurs then
		self:_retryCall(function(): any
			self._lockStore:RemoveAsync(lockKey)
			return nil :: any
		end, Enum.DataStoreRequestType.SetIncrementAsync)
	end
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
					return nil :: any -- someone else owns it now; don't overwrite their lock
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
	local migrations: { MigrationFn } = self._config.migrations
	local currentVersion = fromVersion
	local currentData = data

	while currentVersion < self._config.schemaVersion do
		local migrationFn = migrations[currentVersion] -- index N upgrades from version N to N+1
		if migrationFn then
			-- run migration in pcall so one broken migration doesn't kill the whole load
			-- tradeoff: a failed migration means that version's changes are missing,
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
		-- some version bumps don't need data changes (e.g. adding a new optional field via reconcile)
		currentVersion += 1
	end

	return currentData
end

--------------------------------------------------------------------------------
-- internal: dirty tracking
--------------------------------------------------------------------------------

function DataManager._getDirtyKeys(self: DataManager, profile: Profile): { string }
	-- compare live raw data against the last-saved snapshot to find what changed
	-- we only do top-level key comparison intentionally:
	-- deep comparison of every nested table on every save would be expensive at scale
	-- for nested table changes, the proxy's __newindex marks the top-level key dirty automatically
	-- so callers don't need to do anything special — just write and the proxy handles it
	local dirty = {} :: { string }

	-- forward pass: find keys that exist in current data but differ from the snapshot
	for k, v in pairs(profile._rawData) do
		local saved = profile.savedData[k]
		if saved == nil then
			-- new key added since last save (e.g. from reconcile after a schema change)
			dirty[#dirty + 1] = k
		elseif type(v) ~= type(saved) then
			-- type changed; definitely dirty (e.g. a number replaced with a table)
			-- check type before value because comparing a table to a number would always be dirty
			dirty[#dirty + 1] = k
		elseif type(v) == "table" then
			-- nested table: we rely on dirtyKeys flags set by the proxy's __newindex
			if profile.dirtyKeys[k] then
				dirty[#dirty + 1] = k
			end
		elseif v ~= saved then
			dirty[#dirty + 1] = k
		end
	end

	-- reverse pass: find keys that were deleted from data since last save
	-- can't catch these in the forward pass because they no longer exist in _rawData
	for k in pairs(profile.savedData) do
		if profile._rawData[k] == nil then
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

	if not profile.loaded then
		-- if the initial load never succeeded, profile._rawData may be empty or partially reconciled
		-- writing it would overwrite whatever valid data the player had in the store
		-- so we refuse to save until at least one successful load has happened
		return false
	end

	-- check dirty keys before doing any work
	-- dirty tracking here means: skip the write entirely when nothing changed since last save
	-- note: when we DO write, we still write the full payload — DataStore has no partial-key update API
	-- so the benefit of dirty tracking is skipping unnecessary writes, not reducing payload size
	-- on release we always write regardless — we're about to drop the lock and can't risk
	-- leaving unsaved changes if the player had a mutation that slipped past dirty tracking
	local dirtyKeys = self:_getDirtyKeys(profile)
	if #dirtyKeys == 0 and not isRelease then
		-- nothing changed since last save; skip the write to conserve request budget
		return true
	end

	-- run onBeforeSave middleware on a copy of the raw data, not on the proxy or _rawData directly
	-- we don't want middleware side-effects (like stamping timestamps) to pollute the live data
	local dataToSave = deepCopy(profile._rawData)
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
		-- snapshot _rawData (not dataToSave) because middleware may have modified dataToSave,
		-- and we want the dirty baseline to reflect the actual live data, not middleware output
		profile.savedData = deepCopy(profile._rawData)
		profile.dirtyKeys = {}                  -- clear dirty flags; everything is now in sync with the store
		changeLogClear(profile.changelog)       -- reset ring buffer; log only reflects changes since last save
	else
		self._totalFailedSaves += 1
		warn(string.format("DataManager: failed to save userId %d", profile.userId))
		-- do NOT clear dirtyKeys or changelog on failure so the next attempt includes everything
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
	-- profile.data on the returned profile is a proxy — write to it directly to track changes

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
		return nil -- caller should kick the player with a "please rejoin" message
	end

	local key = tostring(userId)
	local ok, result = self:_retryCall(function(): any
		return self._store:GetAsync(key)
	end, Enum.DataStoreRequestType.GetAsync)

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
	-- done before reconcile so migrations run on the raw saved shape, not a partially-reconciled one
	-- ordering matters: if a migration removes a key, reconcile shouldn't re-add it
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

	-- build the profile record with _rawData holding the actual data
	-- profile.data will be the proxy, set up below after the profile exists
	local profile: Profile = {
		userId            = userId,
		data              = {} :: { [string]: any }, -- placeholder; replaced by proxy below
		_rawData          = rawData,
		savedData         = deepCopy(rawData), -- baseline for dirty tracking; must be independent
		version           = self._config.schemaVersion,
		loaded            = true,
		releasing         = false,
		lockRefreshThread = nil,
		dirtyKeys         = {},
		changelog         = newChangeLog(200), -- ring buffer; 200 entries, O(1) insert, bounded memory
		_proxyCache       = {},                -- nested proxy cache; explicitly cleared on release
	}

	-- now wire up the proxy — profile must exist first so the proxy closures can reference it
	-- from this point on, all reads and writes to profile.data go through the metatable
	profile.data = self:_makeProxy(profile)

	self._profiles[userId] = profile
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
	-- if we released the lock first, another server could load stale data
	-- before our final write landed in the store
	self:_saveProfile(profile, true)

	self:_releaseLock(userId)
	self:_notifyUnlock(userId) -- lets other servers skip the timeout wait

	-- explicitly clear the proxy cache so nested table references don't linger until GC
	-- without this, the cache would hold rawTable references alive even after the profile is gone
	-- clearing here means cleanup happens at a known deterministic point, not whenever GC runs
	table.clear(profile._proxyCache)

	-- remove from loaded profiles last so nothing references a half-released profile
	self._profiles[userId] = nil
end

--------------------------------------------------------------------------------
-- public: data access
--------------------------------------------------------------------------------

function DataManager.getProfile(self: DataManager, userId: UserId): Profile?
	-- direct lookup; returns nil if the player isn't loaded on this server
	-- prefer writing to profile.data directly over using setKey — the proxy handles dirty marking
	return self._profiles[userId]
end

function DataManager.setKey(self: DataManager, userId: UserId, key: Key, value: any)
	-- convenience method for writing a key when you only have the userId, not the profile reference
	-- internally this just writes through the proxy, so dirty marking and change logging still fire
	local profile = self._profiles[userId]
	if not profile then
		warn(string.format("DataManager.setKey: no loaded profile for userId %d", userId))
		return
	end
	-- write through the proxy so __newindex fires and handles dirty + changelog automatically
	-- this is equivalent to profile.data[key] = value
	profile.data[key] = value
end

function DataManager.getKey(self: DataManager, userId: UserId, key: Key): any
	-- convenience method for reading a key when you only have the userId
	local profile = self._profiles[userId]
	if not profile then
		warn(string.format("DataManager.getKey: no loaded profile for userId %d", userId))
		return nil
	end
	return profile.data[key] -- read through the proxy, which falls through to _rawData via __index
end

function DataManager.wipeData(self: DataManager, userId: UserId)
	-- reset a player's data to the template defaults in memory
	-- does NOT release the session or save immediately; wipe persists on next auto-save or leave
	-- typical use cases: ban reset, debug clear, "delete my data" request
	local profile = self._profiles[userId]
	if not profile then
		warn(string.format("DataManager.wipeData: no loaded profile for userId %d", userId))
		return
	end

	-- replace the underlying raw data with a fresh copy of the template
	-- we update _rawData directly here (not through the proxy) because we're replacing the whole
	-- table at once, not writing individual keys — there's no single "old value" to log per key
	profile._rawData = deepCopy(self._config.template)

	-- mark every template key as dirty so the wipe gets written on next save
	-- can't rely on _getDirtyKeys alone here because some keys may have the same value
	-- as savedData (e.g. coins = 0 in both), which would make them look clean
	for k in pairs(self._config.template) do
		profile.dirtyKeys[k] = true
	end

	-- log a wipe entry in the changelog so callers know a full reset happened
	changeLogInsert(profile.changelog, {
		key      = "__wipe__", -- sentinel key; not a real data key
		oldValue = nil,
		newValue = nil,
	} :: ChangeEntry)
end

function DataManager.getChangelog(self: DataManager, userId: UserId): { ChangeEntry }
	-- return all writes since the last successful save, in chronological order
	-- the log is a ring buffer internally; changeLogRead reconstructs the correct order
	-- it is cleared automatically on every successful save
	-- useful for: audit logging, syncing UI on specific key changes, debugging mutations
	local profile = self._profiles[userId]
	if not profile then
		return {}
	end
	-- changeLogRead returns a fresh array in insertion order so external code can't corrupt the buffer
	return changeLogRead(profile.changelog)
end

--------------------------------------------------------------------------------
-- public: stats
--------------------------------------------------------------------------------

function DataManager.getStats(self: DataManager): ManagerStats
	-- count loaded profiles by iterating rather than storing a separate counter
	-- a counter could drift if releases happen during errors; iterating is always accurate
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
