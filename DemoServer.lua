-- DataManager Demo Script

local DataManager = require(script.Parent.DataManager) -- adjust path if you nest the module
local Players = game:GetService("Players")

-- create the manager with a basic config
-- for the demo, coins + level + inventory is enough to show it working
local manager = DataManager.new({
	storeName = "PlayerData_Demo",
	lockStoreName = "PlayerData_Locks",
	schemaVersion = 2,

	-- default data every new player starts with
	template = {
		coins = 0,
		level = 1,
		inventory = {},
		lastSeen = 0,
	},

	-- example migration: v1 didn't have "lastSeen", v2 adds it
	-- index 1 = migration that runs when saved version is 1
	migrations = {
		[1] = function(old)
			old.lastSeen = 0 -- backfill the missing key
			return old
		end,
	},

	autoSaveInterval = 60, -- save everyone every 60 seconds
	lockTimeout = 30,
	maxRetries = 5,

	-- middleware: stamp the save time right before every write
	onBeforeSave = function(data)
		data.lastSeen = os.time()
		return data
	end,

	-- middleware: print a line after every load so you can see it working in output
	onAfterLoad = function(data)
		print("DataManager: loaded data ->", data.coins, "coins, level", data.level)
		return data
	end,
})

-- load profile when player joins
Players.PlayerAdded:Connect(function(player)
	local profile = manager:loadProfile(player.UserId)

	if not profile then
		-- couldn't load — kick the player rather than let them play with no data
		-- this prevents inventory dupes and other data loss bugs
		player:Kick("Failed to load your data. Please rejoin.")
		return
	end

	-- give the player a leaderstats board so you can see the data live in-game
	local leaderstats = Instance.new("Folder")
	leaderstats.Name = "leaderstats"
	leaderstats.Parent = player

	local coinsVal = Instance.new("IntValue")
	coinsVal.Name = "Coins"
	coinsVal.Value = profile.data.coins
	coinsVal.Parent = leaderstats

	local levelVal = Instance.new("IntValue")
	levelVal.Name = "Level"
	levelVal.Value = profile.data.level
	levelVal.Parent = leaderstats

	-- demo: give the player 10 coins every 15 seconds so you can watch saves work
	task.spawn(function()
		while player.Parent do
			task.wait(15)

			-- check profile still exists (player might have left)
			local p = manager:getProfile(player.UserId)
			if not p then break end

			-- update the data through setKey so dirty tracking fires
			local newCoins = p.data.coins + 10
			manager:setKey(player.UserId, "coins", newCoins)

			-- keep leaderstats in sync so you can see it changing
			coinsVal.Value = newCoins
			print(string.format("DataManager: gave %s 10 coins (total: %d)", player.Name, newCoins))
		end
	end)
end)

-- release profile when player leaves
-- this does the final save + lock release
Players.PlayerRemoving:Connect(function(player)
	manager:releaseProfile(player.UserId)
end)

-- also release on server shutdown so data isn't lost when Roblox stops the server
game:BindToClose(function()
	-- BindToClose gives you ~30 seconds to finish
	-- release all loaded profiles before the server dies
	for _, player in ipairs(Players:GetPlayers()) do
		manager:releaseProfile(player.UserId)
	end
end)

-- print stats every 30 seconds so you can see the numbers ticking up in output
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
