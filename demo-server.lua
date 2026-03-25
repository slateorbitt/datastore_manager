-- DataManager Demo Script
-- place this in ServerScriptService as a regular Script
-- the only other thing you need is the DataManager ModuleScript in the same folder

local Players = game:GetService("Players")
local DataManager = require(script.Parent.DataManager)

-- create the manager with a basic config
-- coins + level + inventory demonstrates flat values, nested tables, and migrations all at once
local manager = DataManager.new({
	storeName     = "PlayerData_Demo",
	lockStoreName = "PlayerData_Locks",
	schemaVersion = 2,

	-- default data every new player starts with
	template = {
		coins     = 0,
		level     = 1,
		inventory = {},
		lastSeen  = 0,
	},

	-- example migration: v1 didn't have "lastSeen", v2 adds it
	-- index 1 = the function that upgrades from v1 to v2
	migrations = {
		[1] = function(old)
			old.lastSeen = 0 -- backfill the missing key for any pre-v2 save
			return old
		end,
	},

	autoSaveInterval = 60, -- auto-save all loaded profiles every 60 seconds
	lockTimeout      = 30,
	maxRetries       = 5,

	-- middleware: stamp lastSeen right before every write
	-- runs on a copy so it doesn't pollute the live data table
	onBeforeSave = function(data)
		data.lastSeen = os.time()
		return data
	end,

	-- middleware: print a line after every successful load so you can see it in output
	onAfterLoad = function(data)
		print(string.format("DataManager: loaded — coins: %d, level: %d", data.coins, data.level))
		return data
	end,

	-- fires immediately whenever any key changes through the proxy
	-- used here to keep leaderstats in sync without any manual update calls
	onKeyChanged = function(userId, key, _oldValue, newValue)
		local player = Players:GetPlayerByUserId(userId)
		if not player then return end
		local ls = player:FindFirstChild("leaderstats")
		if not ls then return end
		local stat = ls:FindFirstChild(key == "coins" and "Coins" or key == "level" and "Level" or "")
		if stat then
			stat.Value = newValue
		end
	end,
})

-- load profile when player joins
Players.PlayerAdded:Connect(function(player)
	local profile = manager:loadProfile(player.UserId)

	if not profile then
		-- couldn't load; kick so they don't play with no data
		-- this prevents inventory dupes and other data-loss bugs
		player:Kick("Failed to load your data. Please rejoin.")
		return
	end

	-- set up leaderstats so coins and level are visible on the leaderboard
	local leaderstats = Instance.new("Folder")
	leaderstats.Name = "leaderstats"
	leaderstats.Parent = player

	local coinsVal = Instance.new("IntValue")
	coinsVal.Name  = "Coins"
	coinsVal.Value = profile.data.coins -- read directly through the proxy
	coinsVal.Parent = leaderstats

	local levelVal = Instance.new("IntValue")
	levelVal.Name  = "Level"
	levelVal.Value = profile.data.level
	levelVal.Parent = leaderstats

	-- demo: give the player 10 coins every 15 seconds
	-- writing through the proxy automatically marks "coins" dirty — no setKey needed
	task.spawn(function()
		while player.Parent do
			task.wait(15)

			local p = manager:getProfile(player.UserId)
			if not p then break end

			-- direct proxy write; onKeyChanged fires and updates leaderstats automatically
			p.data.coins += 10

			-- nested table write to show the proxy handles it correctly
			-- this marks "inventory" dirty without any manual flagging
			local inv = p.data.inventory
			inv[#inv + 1] = "item_" .. os.clock()

			print(string.format(
				"DataManager: gave %s 10 coins (total: %d) | inventory size: %d",
				player.Name, p.data.coins, #p.data.inventory
			))
		end
	end)
end)

-- release profile when player leaves
-- this does the final save + lock release
Players.PlayerRemoving:Connect(function(player)
	manager:releaseProfile(player.UserId)
end)

-- release all profiles on server shutdown so data isn't lost
-- BindToClose gives ~30 seconds to finish up
game:BindToClose(function()
	for _, player in ipairs(Players:GetPlayers()) do
		manager:releaseProfile(player.UserId)
	end
end)

-- print stats every 30 seconds so you can watch saves/retries tick up in output
task.spawn(function()
	while true do
		task.wait(30)
		local stats = manager:getStats()
		print(string.format(
			"[DataManager Stats] profiles: %d | saves: %d | loads: %d | retries: %d | failedSaves: %d",
			stats.loadedProfiles,
			stats.totalSaves,
			stats.totalLoads,
			stats.totalRetries,
			stats.totalFailedSaves
		))
	end
end)
