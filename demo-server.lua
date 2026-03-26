-- DemoServer.lua
-- place in ServerScriptService as a regular Script
-- requires DataManager ModuleScript in the same folder

local Players        = game:GetService("Players")
local DataManager    = require(script.Parent.DataManager)

-- manager setup

local manager = DataManager.new({
	storeName        = "PlayerData_Demo",
	lockStoreName    = "PlayerData_Locks",
	schemaVersion    = 2,

	template = {
		coins     = 0,
		level     = 1,
		inventory = {},
		lastSeen  = 0,
	},

	-- v1 → v2: backfill lastSeen for any old saves
	migrations = {
		[1] = function(old)
			old.lastSeen = 0
			return old
		end,
	},

	autoSaveInterval = 60,
	lockTimeout      = 30,
	maxRetries       = 5,

	-- stamp lastSeen on every write (runs on a copy, doesn't affect live data)
	onBeforeSave = function(data)
		data.lastSeen = os.time()
		return data
	end,

	onAfterLoad = function(data)
		print(string.format(
			"[DataManager] loaded — coins: %d  level: %d  lastSeen: %d",
			data.coins, data.level, data.lastSeen
		))
		return data
	end,

	-- keeps leaderstats in sync automatically without any manual update calls
	onKeyChanged = function(userId, key, _old, new)
		local player = Players:GetPlayerByUserId(userId)
		if not player then return end
		local ls = player:FindFirstChild("leaderstats")
		if not ls then return end
		local statName = key == "coins" and "Coins" or key == "level" and "Level" or nil
		if statName then
			local stat = ls:FindFirstChild(statName)
			if stat then stat.Value = new end
		end
	end,
})

-- helpers

local function setupLeaderstats(player: Player, profile)
	local ls = Instance.new("Folder")
	ls.Name   = "leaderstats"
	ls.Parent = player

	local coins      = Instance.new("IntValue")
	coins.Name       = "Coins"
	coins.Value      = profile.data.coins
	coins.Parent     = ls

	local level      = Instance.new("IntValue")
	level.Name       = "Level"
	level.Value      = profile.data.level
	level.Parent     = ls
end

-- coin loop: +10 coins every 15 s, also pushes a timestamped inventory item
-- to demonstrate that nested proxy writes dirty the root key correctly
local function startCoinLoop(player: Player)
	task.spawn(function()
		while player.Parent do
			task.wait(15)

			local p = manager:getProfile(player.UserId)
			if not p then break end

			p.data.coins += 10

			local inv = p.data.inventory
			inv[#inv + 1] = "item_" .. math.floor(os.clock())

			print(string.format(
				"[DataManager] %s +10 coins → %d  |  inventory: %d items",
				player.Name, p.data.coins, #p.data.inventory
			))
		end
	end)
end

-- player lifecycle

Players.PlayerAdded:Connect(function(player)
	local profile = manager:loadProfile(player.UserId)

	if not profile then
		-- no profile = no data = kick so they can rejoin cleanly
		-- keeps them from playing in a stateless session and losing progress
		player:Kick("Couldn't load your data — please rejoin.")
		return
	end

	setupLeaderstats(player, profile)
	startCoinLoop(player)
end)

Players.PlayerRemoving:Connect(function(player)
	manager:releaseProfile(player.UserId)
end)

-- flush everyone on server close (~30 s budget from Roblox)
game:BindToClose(function()
	for _, player in ipairs(Players:GetPlayers()) do
		manager:releaseProfile(player.UserId)
	end
end)

-- stats ticker 

task.spawn(function()
	while true do
		task.wait(30)
		local s = manager:getStats()
		print(string.format(
			"[DataManager Stats]  profiles: %d  saves: %d  loads: %d  retries: %d  failedSaves: %d",
			s.loadedProfiles,
			s.totalSaves,
			s.totalLoads,
			s.totalRetries,
			s.failedSaves       -- fixed: was named totalFailedSaves in old script
		))
	end
end)
