-- stress test server demo.

local Players = game:GetService("Players")
local DataManager = require(script.Parent.DataManager)

local manager = DataManager.new({
	storeName = "PlayerData_Demo",
	lockStoreName = "PlayerData_Locks",
	schemaVersion = 3,

	template = {
		coins = 0,
		level = 1,
		inventory = {},
		lastSeen = 0,
		loginCount = 0,
	},

	migrations = {
		[1] = function(old)
			old.lastSeen = 0
			return old
		end,
		[2] = function(old)
			old.loginCount = 0
			return old
		end,
	},

	autoSaveInterval = 60,
	lockTimeout = 30,
	maxRetries = 5,

	onBeforeSave = function(data)
		data.lastSeen = os.time()
		return data
	end,

	onAfterLoad = function(data)
		data.loginCount += 1
		return data
	end,

	onKeyChanged = function(userId, key, old, new)
		local player = Players:GetPlayerByUserId(userId)
		if not player then return end
		local ls = player:FindFirstChild("leaderstats")
		if not ls then return end
		if key == "coins" then
			local stat = ls:FindFirstChild("Coins")
			if stat then stat.Value = new end
		elseif key == "level" then
			local stat = ls:FindFirstChild("Level")
			if stat then stat.Value = new end
		end
	end,
})

local function setupLeaderstats(player, profile)
	local ls = Instance.new("Folder")
	ls.Name = "leaderstats"
	ls.Parent = player

	local coins = Instance.new("IntValue")
	coins.Name = "Coins"
	coins.Value = profile.data.coins
	coins.Parent = ls

	local level = Instance.new("IntValue")
	level.Name = "Level"
	level.Value = profile.data.level
	level.Parent = ls
end

local function printChangelog(player, profile)
	local changes = manager:getChangelog(player.UserId)
	if #changes == 0 then
		print(string.format("[Changelog] %s - no changes logged", player.Name))
		return
	end
	print(string.format("[Changelog] %s has %d unsaved change(s):", player.Name, #changes))
	for _, entry in ipairs(changes) do
		print(string.format("  key=%s  old=%s  new=%s", tostring(entry.key), tostring(entry.oldValue), tostring(entry.newValue)))
	end
end

local function runShowcase(player, profile)
	task.spawn(function()

		task.wait(3)
		print(string.format("[Demo] %s login #%d", player.Name, profile.data.loginCount))
		print(string.format("[Demo] joined with %d coins, level %d, %d inventory items", profile.data.coins, profile.data.level, #profile.data.inventory))

		task.wait(2)
		profile.data.coins += 50
		print("[Demo] gave 50 coins via proxy write, leaderstats updated automatically")
		printChangelog(player, profile)

		task.wait(2)
		local inv = profile.data.inventory
		inv[#inv + 1] = "sword"
		inv[#inv + 1] = "shield"
		inv[#inv + 1] = "potion"
		print("[Demo] pushed 3 items into nested inventory table, inventory key is now dirty")
		printChangelog(player, profile)

		task.wait(2)
		profile.data.level += 1
		print(string.format("[Demo] level up to %d via proxy, leaderstats updated automatically", profile.data.level))

		task.wait(2)
		profile.data.coins += 25
		profile.data.coins += 25
		profile.data.level += 1
		print("[Demo] two more coin writes + level up, changelog should show all of them:")
		printChangelog(player, profile)

		task.wait(2)
		print("[Demo] wiping data back to template defaults...")
		manager:wipeData(player.UserId)
		print(string.format("[Demo] after wipe - coins: %d  level: %d  inventory: %d items", profile.data.coins, profile.data.level, #profile.data.inventory))

		task.wait(2)
		print("[Demo] stats snapshot:")
		local s = manager:getStats()
		print(string.format("  loadedProfiles=%d  saves=%d  loads=%d  retries=%d  failedSaves=%d",
			s.loadedProfiles, s.totalSaves, s.totalLoads, s.totalRetries, s.failedSaves))

	end)
end

local function onPlayerAdded(player)
	local profile = manager:loadProfile(player.UserId)

	if not profile then
		player:Kick("Couldn't load your data, please rejoin.")
		return
	end

	setupLeaderstats(player, profile)
	runShowcase(player, profile)
end

Players.PlayerAdded:Connect(onPlayerAdded)

-- handles the case in studio where the player already exists before this script runs
for _, player in ipairs(Players:GetPlayers()) do
	task.spawn(onPlayerAdded, player)
end

Players.PlayerRemoving:Connect(function(player)
	manager:releaseProfile(player.UserId)
end)

game:BindToClose(function()
	for _, player in ipairs(Players:GetPlayers()) do
		manager:releaseProfile(player.UserId)
	end
end)
