--!strict
-- player datastore module
-- slateorbitt

--[[
	the whole point of this module is making sure player data never gets corrupted
	or lost when multiple servers are involved. the main ways data gets messed up:

	1. two servers loading the same player and both writing -- session locking fixes this
	2. datastore outages dropping saves -- retry with backoff fixes this
	3. old saves being incompatible after you change your data shape -- migrations fix this

	how the lock works:
	we write {jobId, timestamp} to a separate store before loading anyone's data.
	if another server has a valid (non-expired) lock we back off and return nil.
	we ping the timestamp every N seconds so it doesnt expire while theyre playing.
	on leave we stamp timestamp=0 first (makes the lock look stale to every server immediately)
	then RemoveAsync to clean it up. the 0-stamp is the important part -- roblox doesnt have
	an atomic "delete if i own it" operation so we need two steps.

	profile.data is a proxy, not the actual table. reads fall through to _rawData,
	writes mark the key dirty and append to the changelog. nested tables are also proxied
	so profile.data.inventory[1] = "x" still marks "inventory" dirty without you having
	to do anything special. proxies are cached by raw table ref so we dont allocate
	a new wrapper every time you access the same nested table.

	changelog is a ring buffer capped at MAX_CHANGELOG entries. cleared on save.

	leaderboard integration:
	pass leaderboardKeys = { coins = "GlobalCoins", level = "GlobalLevels" } to mirror
	numeric values to OrderedDataStores on every successful save. only fires if that key
	was actually dirty so you dont burn write budget on unchanged values. the value has to
	be a number or the write gets skipped with a warn.
]]

type UserId = number
type Key = string

type SessionLock = {
	jobId: string,
	timestamp: number,
}

type MigrationFn = (old: { [string]: any }) -> { [string]: any }
type MiddlewareFn = (data: { [string]: any }) -> { [string]: any }?

type ChangeEntry = {
	key: string,
	oldValue: any,
	newValue: any,
}

type ManagerConfig = {
	storeName: string,
	lockStoreName: string,
	template: { [string]: any },
	schemaVersion: number,
	migrations: { MigrationFn }?,
	autoSaveInterval: number?,  -- 0 to disable
	lockTimeout: number?,
	maxRetries: number?,
	onBeforeSave: MiddlewareFn?,
	onAfterLoad: MiddlewareFn?,
	onKeyChanged: ((userId: UserId, key: string, old: any, new: any) -> ())?,
	leaderboardKeys: { [string]: string }?, -- maps data key -> OrderedDataStore name
}

-- resolved version with no optionals so we dont need nil checks everywhere later
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
	onKeyChanged: ((userId: UserId, key: string, old: any, new: any) -> ())?,
	leaderboardKeys: { [string]: string }?,
}

-- ring buffer for changelog. O(1) inserts, fixed memory, wraps when full
type ChangeLog = {
	buf: { ChangeEntry? },
	head: number,
	count: number,
	cap: number,
}

type Profile = {
	userId: UserId,
	data: { [string]: any },      -- THIS IS THE PROXY not _rawData, use this for reads/writes
	_rawData: { [string]: any },
	savedData: { [string]: any },
	version: number,
	loaded: boolean,
	releasing: boolean,
	lockThread: thread?,
	dirtyKeys: { [string]: boolean },
	changelog: ChangeLog,
	_proxyCache: { [any]: any },  -- raw table ref -> proxy wrapper, cleared on release
}

type ManagerStats = {
	loadedProfiles: number,
	totalSaves: number,
	totalLoads: number,
	totalRetries: number,
	failedSaves: number,
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
	_lbStores: { [string]: OrderedDataStore }, -- leaderboard stores, keyed by data key name
	_saves: number,
	_loads: number,
	_retries: number,
	_failedSaves: number,

	loadProfile:    (self: DataManager, userId: UserId) -> Profile?,
	releaseProfile: (self: DataManager, userId: UserId) -> (),
	getProfile:     (self: DataManager, userId: UserId) -> Profile?,
	setKey:         (self: DataManager, userId: UserId, key: Key, value: any) -> (),
	getKey:         (self: DataManager, userId: UserId, key: Key) -> any,
	wipeData:       (self: DataManager, userId: UserId) -> (),
	getChangelog:   (self: DataManager, userId: UserId) -> { ChangeEntry },
	getStats:       (self: DataManager) -> ManagerStats,

	_retry:         (self: DataManager, fn: () -> any, rt: Enum.DataStoreRequestType) -> (boolean, any),
	_acquireLock:   (self: DataManager, userId: UserId) -> boolean,
	_releaseLock:   (self: DataManager, userId: UserId) -> (),
	_refreshLock:   (self: DataManager, userId: UserId) -> (),
	_save:          (self: DataManager, profile: Profile, final: boolean?) -> boolean,
	_migrate:       (self: DataManager, data: { [string]: any }, from: number) -> { [string]: any },
	_dirty:         (self: DataManager, profile: Profile) -> { string },
	_proxy:         (self: DataManager, profile: Profile) -> { [string]: any },
	_autoSave:      (self: DataManager) -> (),
	_pingUnlock:    (self: DataManager, userId: UserId) -> (),
	_flushLeaderboards: (self: DataManager, profile: Profile) -> (),
}

-- how many changelog entries to keep between saves
-- both _proxy and loadProfile use this so changing it here applies everywhere
local MAX_CHANGELOG = 200

local function deepCopy(t: { [string]: any }): { [string]: any }
	local c = {} :: { [string]: any }
	for k, v in pairs(t) do
		c[k] = if type(v) == "table" then deepCopy(v) else v
	end
	return c
end

local function fill(data: { [string]: any }, template: { [string]: any }): { [string]: any }
	for k, v in pairs(template) do
		if data[k] == nil then
			data[k] = if type(v) == "table" then deepCopy(v) else v
		end
	end
	return data
end

local function newLog(cap: number): ChangeLog
	return { buf = table.create(cap) :: { ChangeEntry? }, head = 1, count = 0, cap = cap }
end

local function pushLog(log: ChangeLog, e: ChangeEntry)
	log.buf[log.head] = e
	log.head = log.head % log.cap + 1
	if log.count < log.cap then log.count += 1 end
end

local function readLog(log: ChangeLog): { ChangeEntry }
	local out = {} :: { ChangeEntry }
	if log.count < log.cap then
		for i = 1, log.count do
			local e = log.buf[i]; if e then out[#out+1] = e end
		end
	else
		local i = log.head
		for _ = 1, log.cap do
			local e = log.buf[i]; if e then out[#out+1] = e end
			i = i % log.cap + 1
		end
	end
	return out
end

local function clearLog(log: ChangeLog)
	table.clear(log.buf :: any)
	log.head = 1; log.count = 0
end

function DataManager.new(config: ManagerConfig): DataManager
	assert(config.storeName and #config.storeName > 0, "storeName is required")
	assert(config.lockStoreName and #config.lockStoreName > 0, "lockStoreName is required")
	assert(config.storeName ~= config.lockStoreName, "storeName and lockStoreName cant be the same store or lock records get corrupted")
	assert(type(config.template) == "table", "template has to be a table")
	assert(type(config.schemaVersion) == "number" and config.schemaVersion >= 1, "schemaVersion must be >= 1")

	if config.autoSaveInterval ~= nil then
		assert(config.autoSaveInterval >= 0, "autoSaveInterval cant be negative (pass 0 to disable it)")
		assert(config.autoSaveInterval <= 3600, "autoSaveInterval shouldnt be over an hour, risk of data loss on crash")
	end
	if config.lockTimeout ~= nil then
		assert(config.lockTimeout >= 5, "lockTimeout below 5s will cause false expiries under normal server latency")
		assert(config.lockTimeout <= 300, "lockTimeout over 5min means very slow recovery if a server crashes")
	end
	if config.maxRetries ~= nil then
		assert(config.maxRetries >= 1, "maxRetries has to be at least 1")
		assert(config.maxRetries <= 10, "maxRetries over 10 will block threads for too long during outages")
	end

	local self = setmetatable({}, DataManager)
	self._store     = DataStoreService:GetDataStore(config.storeName)
	self._lockStore = DataStoreService:GetDataStore(config.lockStoreName)

	-- open an OrderedDataStore for each leaderboard key upfront
	-- GetOrderedDataStore doesnt actually connect until the first write so this is cheap
	self._lbStores = {} :: { [string]: OrderedDataStore }
	if config.leaderboardKeys then
		for dataKey, storeName in pairs(config.leaderboardKeys) do
			self._lbStores[dataKey] = DataStoreService:GetOrderedDataStore(storeName)
		end
	end

	self._config = {
		storeName        = config.storeName,
		lockStoreName    = config.lockStoreName,
		template         = deepCopy(config.template),
		schemaVersion    = config.schemaVersion,
		migrations       = config.migrations or {},
		autoSaveInterval = if config.autoSaveInterval ~= nil then config.autoSaveInterval else 60,
		lockTimeout      = if config.lockTimeout ~= nil then config.lockTimeout else 30,
		maxRetries       = if config.maxRetries ~= nil then config.maxRetries else 5,
		onBeforeSave     = config.onBeforeSave,
		onAfterLoad      = config.onAfterLoad,
		onKeyChanged     = config.onKeyChanged,
		leaderboardKeys  = config.leaderboardKeys,
	} :: ResolvedConfig

	self._profiles   = {} :: { [UserId]: Profile }
	self._saves       = 0
	self._loads       = 0
	self._retries     = 0
	self._failedSaves = 0

	local dm = self :: DataManager
	if dm._config.autoSaveInterval > 0 then dm:_autoSave() end

	-- if another server releases a player's lock it pings us so we dont have to
	-- wait out the full timeout before retrying. pcall bc MessagingService is unreliable
	-- in studio and private servers
	pcall(function()
		MessagingService:SubscribeAsync("DM_Unlock", function(msg)
			local uid = tonumber(msg.Data)
			if not uid then return end
			local p = dm._profiles[uid :: number]
			if p and not p.releasing then dm:releaseProfile(uid :: number) end
		end)
	end)

	return dm
end

function DataManager._proxy(self: DataManager, profile: Profile): { [string]: any }
	-- builds a proxy table in front of profile._rawData
	-- all writes through the proxy automatically mark the key dirty and log the change
	--
	-- the tricky part is nested tables. if you just proxy the root, then:
	--   profile.data.inventory[1] = "sword"
	-- wont fire __newindex on the root proxy because that write is happening
	-- on the inventory table, not on profile.data directly. to fix this, __index
	-- wraps any returned table in its own nested proxy that marks the root key dirty.
	-- works at any depth. profile.data.a.b.c = x marks "a" dirty.
	--
	-- proxy cache maps raw table reference -> wrapper so we dont allocate a new one
	-- every time you do profile.data.inventory[1] vs [2] in the same frame

	local uid = profile.userId
	local cache = profile._proxyCache

	-- typed as any to avoid the union type strict creates for recursive locals
	-- (forward declare then assign = union of nil | fn, cant call that)
	local nestedProxy: any
	nestedProxy = function(raw: { [string]: any }, rootKey: string): { [string]: any }
		if cache[raw] then return cache[raw] end

		local t, m = {}, {}

		m.__index = function(_: any, k: string): any
			local v = raw[k]
			return if type(v) == "table" then nestedProxy(v, rootKey) else v
		end

		m.__newindex = function(_: any, k: string, nv: any)
			local ov = raw[k]
			raw[k] = nv
			profile.dirtyKeys[rootKey] = true
			pushLog(profile.changelog, { key = rootKey, oldValue = ov, newValue = nv } :: ChangeEntry)
			local cb = self._config.onKeyChanged
			if cb then
				local ok, err = (pcall :: (any, ...any) -> (boolean, any))(cb, uid, rootKey, ov, nv)
				if not ok then warn("DataManager: onKeyChanged threw for key '" .. rootKey .. "': " .. tostring(err)) end
			end
		end

		-- strict complains about # on string-keyed tables so we cast
		m.__len = function(_: any): number return #(raw :: { any }) end

		local px = setmetatable(t, m) :: any
		cache[raw] = px
		return px
	end

	local root, rm = {}, {}

	rm.__index = function(_: any, key: string): any
		local v = profile._rawData[key]
		return if type(v) == "table" then nestedProxy(v, key) else v
	end

	rm.__newindex = function(_: any, key: string, nv: any)
		local ov = profile._rawData[key]
		profile._rawData[key] = nv
		profile.dirtyKeys[key] = true
		if type(nv) == "table" then cache[nv] = nil end  -- old proxy wraps old ref, needs to be rebuilt
		pushLog(profile.changelog, { key = key, oldValue = ov, newValue = nv } :: ChangeEntry)
		local cb = self._config.onKeyChanged
		if cb then
			local ok, err = (pcall :: (any, ...any) -> (boolean, any))(cb, uid, key, ov, nv)
			if not ok then warn("DataManager: onKeyChanged threw for key '" .. key .. "': " .. tostring(err)) end
		end
	end

	rm.__len = function(_: any): number return #(profile._rawData :: { any }) end

	return setmetatable(root, rm) :: any
end

function DataManager._retry(self: DataManager, fn: () -> any, rt: Enum.DataStoreRequestType): (boolean, any)
	local maxR: number = self._config.maxRetries
	local lastErr: any = nil

	for attempt = 1, maxR do
		if DataStoreService:GetRequestBudgetForRequestType(rt) <= 0 then
			-- no budget, wait a bit. counts as a retry
			task.wait(2)
			self._retries += 1
			continue
		end

		local ok, res = pcall(fn)
		if ok then return true, res end

		lastErr = res
		if attempt < maxR then
			self._retries += 1
			-- exponential backoff with jitter so servers dont all retry at the same time
			task.wait(math.min(0.5 * 2^(attempt-1), 16) + math.random() * 0.5)
		end
	end

	warn(string.format("DataManager: gave up after %d retries -- %s", maxR, tostring(lastErr)))
	return false, lastErr
end

function DataManager._acquireLock(self: DataManager, userId: UserId): boolean
	-- UpdateAsync is atomic unlike SetAsync. with SetAsync two servers could both
	-- read "no lock" and then both write their claim at the same time.
	-- UpdateAsync callbacks are serialized so only one server can claim at a time.
	local lockKey = "lock_" .. userId
	local timeout: number = self._config.lockTimeout
	local myJob: string = game.JobId
	local got = false

	local ok, _ = self:_retry(function(): any
		self._lockStore:UpdateAsync(lockKey, function(cur: any): any
			if cur ~= nil then
				local lk = cur :: SessionLock
				if lk.jobId ~= myJob and os.time() - lk.timestamp < timeout then
					return nil :: any -- another server has a valid lock, abort
				end
				-- either its our own lock (refresh) or its stale (crashed server)
			end
			got = true
			return { jobId = myJob, timestamp = os.time() } :: any
		end)
		return nil :: any
	end, Enum.DataStoreRequestType.UpdateAsync)

	return ok and got
end

function DataManager._releaseLock(self: DataManager, userId: UserId)
	-- important: UpdateAsync returning nil cancels the write, it does NOT delete the key.
	-- theres no single atomic "delete if i own it" in roblox datastores so we do two steps:
	--
	-- step 1 (atomic): stamp timestamp=0. this makes the lock look permanently stale to
	--   every other server so they can claim it immediately even if step 2 never happens.
	--
	-- step 2 (cleanup): RemoveAsync to actually delete the key.
	--   non-critical. if this fails the timestamp=0 already did the real work.
	--   RemoveAsync uses the SetIncrementAsync budget bucket per roblox docs.

	local lockKey = "lock_" .. userId
	local myJob: string = game.JobId
	local ours = false

	self:_retry(function(): any
		self._lockStore:UpdateAsync(lockKey, function(cur: any): any
			if cur == nil then return nil :: any end
			local lk = cur :: SessionLock
			if lk.jobId ~= myJob then return nil :: any end
			ours = true
			return { jobId = myJob, timestamp = 0 } :: any
		end)
		return nil :: any
	end, Enum.DataStoreRequestType.UpdateAsync)

	if ours then
		self:_retry(function(): any
			self._lockStore:RemoveAsync(lockKey)
			return nil :: any
		end, Enum.DataStoreRequestType.SetIncrementAsync)
	end
end

function DataManager._refreshLock(self: DataManager, userId: UserId)
	local lockKey = "lock_" .. userId
	local myJob: string = game.JobId
	-- refresh at half the timeout so normal clock drift between servers cant cause false expiry
	local interval: number = math.floor(self._config.lockTimeout / 2)

	while true do
		task.wait(interval)
		local p = self._profiles[userId]
		if not p or p.releasing then break end

		self:_retry(function(): any
			self._lockStore:UpdateAsync(lockKey, function(cur: any): any
				if cur == nil then return nil :: any end
				local lk = cur :: SessionLock
				if lk.jobId ~= myJob then return nil :: any end
				return { jobId = myJob, timestamp = os.time() } :: any
			end)
			return nil :: any
		end, Enum.DataStoreRequestType.UpdateAsync)
	end
end

function DataManager._pingUnlock(self: DataManager, userId: UserId)
	pcall(function()
		MessagingService:PublishAsync("DM_Unlock", tostring(userId))
	end)
end

function DataManager._migrate(self: DataManager, data: { [string]: any }, from: number): { [string]: any }
	local migrations = self._config.migrations
	local v = from
	local d = data

	while v < self._config.schemaVersion do
		local fn = migrations[v]
		if fn then
			-- run in pcall so one broken migration doesnt lock everyone out
			local ok, res = pcall(fn, deepCopy(d))
			if ok and type(res) == "table" then
				d = res
			else
				warn(string.format("DataManager: migration v%d->v%d failed, skipping: %s", v, v+1, tostring(res)))
			end
		end
		v += 1
	end

	return d
end

function DataManager._dirty(self: DataManager, profile: Profile): { string }
	-- compares _rawData against savedData to find what changed
	-- top-level only. deep comparing nested tables per save is too slow.
	-- nested changes get caught by the proxy setting dirtyKeys on the root key.
	local dirty = {} :: { string }

	for k, v in pairs(profile._rawData) do
		local sv = profile.savedData[k]
		if sv == nil then
			dirty[#dirty+1] = k
		elseif type(v) ~= type(sv) then
			dirty[#dirty+1] = k
		elseif type(v) == "table" then
			if profile.dirtyKeys[k] then dirty[#dirty+1] = k end
		elseif v ~= sv then
			dirty[#dirty+1] = k
		end
	end

	for k in pairs(profile.savedData) do
		if profile._rawData[k] == nil then dirty[#dirty+1] = k end
	end

	return dirty
end

function DataManager._flushLeaderboards(self: DataManager, profile: Profile)
	-- called after a successful save to push dirty numeric values to their OrderedDataStores
	-- only fires for keys that were actually dirty -- no point writing the same score twice
	-- skips non-numeric values with a warn since OrderedDataStore only accepts integers
	-- each write goes through _retry so budget throttling still applies
	local lbKeys = self._config.leaderboardKeys
	if not lbKeys then return end

	local key = tostring(profile.userId)

	for dataKey, store in pairs(self._lbStores) do
		-- only bother if this key actually changed in the save we just did
		if not profile.dirtyKeys[dataKey] then continue end

		local val = profile._rawData[dataKey]
		if type(val) ~= "number" then
			warn(string.format("DataManager: leaderboard key '%s' is not a number (got %s), skipping", dataKey, type(val)))
			continue
		end

		-- OrderedDataStore only takes integers. math.floor so a float like 9.7 doesnt error.
		local score = math.floor(val)
		self:_retry(function(): any
			store:SetAsync(key, score)
			return nil :: any
		end, Enum.DataStoreRequestType.SetIncrementAsync)
	end
end

function DataManager._save(self: DataManager, profile: Profile, final: boolean?): boolean
	if not profile.loaded then return false end

	-- skip the write if nothing changed. on the final save we always write because
	-- we're about to release the lock and cant risk leaving unsaved changes behind.
	local dirtyList = self:_dirty(profile)
	if #dirtyList == 0 and not final then return true end

	local toSave = deepCopy(profile._rawData)
	if self._config.onBeforeSave then
		local r = self._config.onBeforeSave(toSave)
		if r ~= nil then toSave = r end
	end

	local ok, _ = self:_retry(function(): any
		self._store:SetAsync(tostring(profile.userId), {
			data    = toSave,
			version = self._config.schemaVersion,
		})
		return nil :: any
	end, Enum.DataStoreRequestType.SetIncrementAsync)

	if ok then
		self._saves += 1
		-- snapshot _rawData not toSave. middleware might have changed toSave and we
		-- dont want those changes to affect what we consider "clean" for next dirty check.
		profile.savedData = deepCopy(profile._rawData)

		-- push leaderboard updates before clearing dirtyKeys so _flushLeaderboards
		-- can still check which keys changed in this save
		self:_flushLeaderboards(profile)

		profile.dirtyKeys = {}
		clearLog(profile.changelog)
	else
		self._failedSaves += 1
		warn(string.format("DataManager: save failed for userId %d", profile.userId))
		-- dont clear dirty keys on failure so next attempt includes everything
	end

	return ok
end

function DataManager._autoSave(self: DataManager)
	local interval: number = self._config.autoSaveInterval
	task.spawn(function()
		while true do
			task.wait(interval)
			for _, p in pairs(table.clone(self._profiles)) do
				if not p.releasing then self:_save(p, false) end
			end
		end
	end)
end

function DataManager.loadProfile(self: DataManager, userId: UserId): Profile?
	if self._profiles[userId] then return self._profiles[userId] end

	-- lock BEFORE reading. if you read first, two servers can both GetAsync
	-- on a new player and both get nil, then both create "fresh" data simultaneously.
	if not self:_acquireLock(userId) then
		warn(string.format("DataManager: couldnt get lock for userId %d, player probably active on another server", userId))
		return nil
	end

	local ok, result = self:_retry(function(): any
		return self._store:GetAsync(tostring(userId))
	end, Enum.DataStoreRequestType.GetAsync)

	if not ok then
		self:_releaseLock(userId) -- release so they can rejoin, dont leave a dangling lock
		warn(string.format("DataManager: GetAsync failed for userId %d", userId))
		return nil
	end

	local rawData: { [string]: any }
	local savedVer: number = 1

	if result == nil then
		rawData = deepCopy(self._config.template)
	else
		local pl = result :: { data: { [string]: any }, version: number }
		rawData  = pl.data    or deepCopy(self._config.template)
		savedVer = pl.version or 1
	end

	-- migrate before fill so a migration can remove keys that fill would re-add
	if savedVer < self._config.schemaVersion then
		rawData = self:_migrate(rawData, savedVer)
	end

	rawData = fill(rawData, self._config.template)

	if self._config.onAfterLoad then
		local r = self._config.onAfterLoad(deepCopy(rawData))
		if r ~= nil then rawData = r end
	end

	local profile: Profile = {
		userId      = userId,
		data        = {} :: { [string]: any }, -- placeholder, gets replaced by proxy below
		_rawData    = rawData,
		savedData   = deepCopy(rawData),
		version     = self._config.schemaVersion,
		loaded      = true,
		releasing   = false,
		lockThread  = nil,
		dirtyKeys   = {},
		changelog   = newLog(MAX_CHANGELOG),
		_proxyCache = {},
	}

	-- proxy has to be built after profile exists because the closures inside reference it
	profile.data = self:_proxy(profile)

	self._profiles[userId] = profile
	self._loads += 1

	profile.lockThread = task.spawn(function()
		self:_refreshLock(userId)
	end)

	return profile
end

function DataManager.releaseProfile(self: DataManager, userId: UserId)
	local profile = self._profiles[userId]
	if not profile then return end

	profile.releasing = true

	if profile.lockThread then
		task.cancel(profile.lockThread)
		profile.lockThread = nil
	end

	-- order matters: save first while we still have the lock,
	-- then release the lock, then ping other servers
	self:_save(profile, true)
	self:_releaseLock(userId)
	self:_pingUnlock(userId)

	table.clear(profile._proxyCache) -- drop ref so GC doesnt have to hold on to it
	self._profiles[userId] = nil
end

function DataManager.getProfile(self: DataManager, userId: UserId): Profile?
	return self._profiles[userId]
end

function DataManager.setKey(self: DataManager, userId: UserId, key: Key, value: any)
	local p = self._profiles[userId]
	if not p then warn("DataManager.setKey: no loaded profile for userId " .. userId); return end
	p.data[key] = value
end

function DataManager.getKey(self: DataManager, userId: UserId, key: Key): any
	local p = self._profiles[userId]
	if not p then warn("DataManager.getKey: no loaded profile for userId " .. userId); return nil end
	return p.data[key]
end

function DataManager.wipeData(self: DataManager, userId: UserId)
	local profile = self._profiles[userId]
	if not profile then warn("DataManager.wipeData: no loaded profile for userId " .. userId); return end

	local prev = profile._rawData
	profile._rawData = deepCopy(self._config.template)

	-- mark everything dirty manually because _dirty() wont catch keys that reset to
	-- the same value as their default (e.g. coins was 0, template says 0 too, looks clean)
	for k in pairs(self._config.template) do
		profile.dirtyKeys[k] = true
	end

	local cb = self._config.onKeyChanged
	if cb then
		-- fire for each key in new data
		for k, nv in pairs(profile._rawData) do
			local ov = prev[k]
			-- tables: always fire since we cant cheaply compare contents
			-- primitives: only fire if value actually changed
			if type(nv) == "table" or type(ov) == "table" or ov ~= nv then
				local ok, err = (pcall :: (any, ...any) -> (boolean, any))(cb, userId, k, ov, nv)
				if not ok then warn("DataManager: wipeData onKeyChanged threw for key '" .. k .. "': " .. tostring(err)) end
			end
		end
		-- also fire for keys that got deleted (existed before but arent in template)
		for k, ov in pairs(prev) do
			if profile._rawData[k] == nil then
				local ok, err = (pcall :: (any, ...any) -> (boolean, any))(cb, userId, k, ov, nil)
				if not ok then warn("DataManager: wipeData onKeyChanged threw for key '" .. k .. "': " .. tostring(err)) end
			end
		end
	end

	pushLog(profile.changelog, { key = "__wipe__", oldValue = nil, newValue = nil } :: ChangeEntry)
end

function DataManager.getChangelog(self: DataManager, userId: UserId): { ChangeEntry }
	local p = self._profiles[userId]
	return if p then readLog(p.changelog) else {}
end

function DataManager.getStats(self: DataManager): ManagerStats
	local n = 0
	for _ in pairs(self._profiles) do n += 1 end
	return {
		loadedProfiles = n,
		totalSaves     = self._saves,
		totalLoads     = self._loads,
		totalRetries   = self._retries,
		failedSaves    = self._failedSaves,
	}
end

return DataManager
