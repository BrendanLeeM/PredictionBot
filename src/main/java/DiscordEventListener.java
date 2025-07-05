import Model.DiscordServer;
import Model.GambleData;
import Model.User;
import Utility.Constants;
import Utility.DynamoDBHelper;
import Utility.GlobalCommandRegistrar;
import discord4j.common.util.Snowflake;
import discord4j.core.DiscordClient;
import discord4j.core.GatewayDiscordClient;
import discord4j.core.event.domain.interaction.*;
import discord4j.core.object.component.*;
import discord4j.core.object.entity.Message;
import discord4j.core.object.entity.Role;
import discord4j.discordjson.json.*;
import discord4j.discordjson.possible.Possible;
import discord4j.rest.entity.RestMessage;
import io.netty.util.internal.StringUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.utils.StringUtils;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class DiscordEventListener {

    private DiscordClient client;
    private static DynamoDBHelper dynamoDBHelper;
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd");
    private static Logger logger = LogManager.getLogger(DiscordEventListener.class);

    public DiscordEventListener(DiscordClient client, DynamoDBHelper dynamoDBHelper) {
        this.client = client;
        this.dynamoDBHelper = dynamoDBHelper;
    }

    private static long calculateInitialDelay() {
        // Set the time zone to Central Standard Time (CST)
        TimeZone cstTimeZone = TimeZone.getTimeZone("CST");

        // Get the current time in CST
        Calendar now = Calendar.getInstance(cstTimeZone);

        // Set the desired time for the task (e.g., 2 AM CST)
        Calendar nextRun = Calendar.getInstance(cstTimeZone);
        nextRun.set(Calendar.HOUR_OF_DAY, 2);
        nextRun.set(Calendar.MINUTE, 0);
        nextRun.set(Calendar.SECOND, 0);
        nextRun.set(Calendar.MILLISECOND, 0);

        // If the desired time is before the current time, schedule for the next day
        if (now.after(nextRun)) {
            nextRun.add(Calendar.DAY_OF_MONTH, 1);
        }

        return (nextRun.getTimeInMillis() - now.getTimeInMillis());
    }


    public Void beginListen() {
        logger.info("Launching listener...");
        final GatewayDiscordClient gateway = client.login().block();
        registerCommands(gateway);

        // Schedule the message sending task
        Timer timer = new Timer();
        long delay = calculateInitialDelay();
        timer.scheduleAtFixedRate(new ResetPointsTask(client), delay, Duration.ofHours(24).toMillis());


        Mono<Void> slashCommandHandler = gateway.on(ChatInputInteractionEvent.class, event -> {
            String authorId = event.getInteraction().getUser().getId().asString();
            String guildId = event.getInteraction().getGuildId().get().asString();
            String guildName = event.getInteraction().getGuild().block().getName();
            String username = event.getInteraction().getUser().getUsername().replace("-", "");
            switch (event.getCommandName().toUpperCase()) {
                case "MYPOINTS":
                    try {
                        long startTime = System.currentTimeMillis();
                        Mono<Void> response = event.deferReply().withEphemeral(true).then(myPoints(event, guildId, authorId, username, guildName)).then();
                        long endTime = System.currentTimeMillis();
                        long totalTime = endTime - startTime;
                        logger.info("Finished " + event.getCommandName() + " in " + totalTime + "ms for guild: " + guildName);
                        return response;
                    } catch (Exception e) {
                        logger.info("Error with mypoints command for authorId {}, guildId {}, exception {}", authorId, guildId, e.getMessage());
                        logger.info(e.getMessage());
                        return event.editReply("We are having an issue getting points right now, please try again later. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb").then();
                    }
                case "STARTPREDICTION":
                    try {
                        long startTime = System.currentTimeMillis();
                        String title = event.getOption("title").get().getValue().get().asString().replace("-", "");
                        String option1 = event.getOption("option1").get().getValue().get().asString().replace("-", "");
                        String option2 = event.getOption("option2").get().getValue().get().asString().replace("-", "");
                        Mono<Void> response = event.deferReply().withEphemeral(true).then(startGamble(event, guildId, authorId, username, guildName, title, option1, option2)).then();
                        long endTime = System.currentTimeMillis();
                        long totalTime = endTime - startTime;
                        logger.info("Finished " + event.getCommandName() + " in " + totalTime + "ms for guild: " + guildName);
                        return response;
                    } catch (Exception e) {
                        logger.info("Error with startprediction command for authorId {}, guildId {}, exception {}", authorId, guildId, e.getMessage());

                    }
                case "DAILYPOINTS":
                    try {
                        long startTime = System.currentTimeMillis();
                        Mono<Void> response = event.deferReply().withEphemeral(true).then(dailyPoints(guildName, guildId, authorId, event, username, 0)).then();
                        long endTime = System.currentTimeMillis();
                        long totalTime = endTime - startTime;
                        logger.info("Finished " + event.getCommandName() + " in " + totalTime + "ms for guild: " + guildName);
                        return response;
                    } catch (Exception e) {
                        logger.info("Error with dailypoints command for authorId {}, guildId {}, exception {}", authorId, guildId, e.getMessage());
                        logger.info(e.getMessage());
                        return event.editReply("Issue giving points. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb").then();
                    }
                case "LEADERBOARD":
                    try {
                        long startTime = System.currentTimeMillis();
                        Mono<Void> response = event.deferReply().withEphemeral(true).then(leaderboard(guildId, event, authorId, username, guildName, false)).then();
                        long endTime = System.currentTimeMillis();
                        long totalTime = endTime - startTime;
                        logger.info("Finished " + event.getCommandName() + " in " + totalTime + "ms for guild: " + guildName);
                        return response;
                    } catch (Exception e) {
                        logger.info("Error with leaderboard command for authorId {}, guildId {}, exception {}", authorId, guildId, e.getMessage());
                    }
                case "CONFIGURE":
                    try {
                        long startTime = System.currentTimeMillis();
                        Mono<Void> response = displayConfigureModal(event, authorId, guildId, guildName, username);
                        long endTime = System.currentTimeMillis();
                        long totalTime = endTime - startTime;
                        logger.info("Finished " + event.getCommandName() + " in " + totalTime + "ms for guild: " + guildName);
                        return response;
                    } catch (Exception e) {
                        logger.info("Error configuring settings for authorId {}, guildId {}", authorId, guildId);
                        logger.info(e.getMessage());
                        return event.editReply("Issue displaying configuring screen. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb").then();
                    }
                case "HELP":
                    try {
                        return event.reply().withEphemeral(true).withContent("Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb");
                    } catch (Exception e) {
                        logger.info("Error with help command for authorId {}, guildId {}, exception {}", authorId, guildId, e.getMessage());
                        return event.reply().withEphemeral(true).withContent("Unable to fulfill request. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb");
                    }
                case "CREATOR":
                    try {
                        return event.deferReply().withEphemeral(true).then(creator(event)).then();
                    } catch (Exception e) {
                        if (e.getMessage() != null && (e.getMessage().contains("50013") || e.getMessage().contains("50001"))){
                            return event.editReply("I need permission to send messages in this channel.").then();
                        }
                        logger.info("Error with creator command for authorId {}, guildId {}, exception {}", authorId, guildId, e.getMessage());
                        return event.editReply().withContent("Unable to fulfill request. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb").then();
                    }
                case "COINFLIP":
                    try {
                        Double randomNum = Math.random();
                        if (randomNum < .5) {
                            return event.reply().withEphemeral(false).withContent("Heads!");

                        } else if (randomNum > .5) {
                            return event.reply().withEphemeral(false).withContent("Tails!");
                        } else {
                            return event.reply().withEphemeral(false).withContent("Coin landed on its side...");

                        }
                    } catch (Exception e) {
                        if (e.getMessage() != null && (e.getMessage().contains("50013") || e.getMessage().contains("50001"))){
                            return event.editReply("I need permission to send messages in this channel.").then();
                        }
                        logger.info("Error with coinflip command for authorId {}, guildId {}, exception {}", authorId, guildId, e.getMessage());
                        return event.reply().withEphemeral(true).withContent("Unable to fulfill request. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb");
                    }
                case "RESETPOINTS":
                    try {
                        long startTime = System.currentTimeMillis();
                        Mono<Void> response = event.deferReply().withEphemeral(true).then(reset(event, authorId, guildId, guildName, username, 0)).then();
                        long endTime = System.currentTimeMillis();
                        long totalTime = endTime - startTime;
                        logger.info("Finished " + event.getCommandName() + " in " + totalTime + "ms for guild: " + guildName);
                        return response;
                    } catch (Exception e) {
                        logger.info("Error with reset command for authorId {}, guildId {}, exception {}", authorId, guildId, e.getMessage());
                        return event.editReply("Issue resetting. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb").then();
                    }
                case "AWARDPOINTS":
                    try {
                        long startTime = System.currentTimeMillis();
                        Mono<Void> response = event.deferReply().withEphemeral(true).then(awardPoints(event, authorId, guildId, guildName, username, 0)).then();
                        long endTime = System.currentTimeMillis();
                        long totalTime = endTime - startTime;
                        logger.info("Finished " + event.getCommandName() + " in " + totalTime + "ms for guild: " + guildName);
                        return response;
                    } catch (Exception e) {
                        logger.info("Error with reset command for authorId {}, guildId {}, exception {}", authorId, guildId, e.getMessage());
                        return event.editReply("Issue resetting. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb").then();
                    }
                case "REMOVEPOINTS":
                    try {
                        long startTime = System.currentTimeMillis();
                        Mono<Void> response = event.deferReply().withEphemeral(true).then(removePoints(event, authorId, guildId, guildName, username, 0)).then();
                        long endTime = System.currentTimeMillis();
                        long totalTime = endTime - startTime;
                        logger.info("Finished " + event.getCommandName() + " in " + totalTime + "ms for guild: " + guildName);
                        return response;
                    } catch (Exception e) {
                        logger.info("Error with reset command for authorId {}, guildId {}, exception {}", authorId, guildId, e.getMessage());
                        return event.editReply("Issue resetting. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb").then();
                    }
                case "REMOVEUSER":
                    try {
                        long startTime = System.currentTimeMillis();
                        Mono<Void> response = event.deferReply().withEphemeral(true).then(removeUser(event, authorId, guildId, guildName, username, 0)).then();
                        long endTime = System.currentTimeMillis();
                        long totalTime = endTime - startTime;
                        logger.info("Finished " + event.getCommandName() + " in " + totalTime + "ms for guild: " + guildName);
                        return response;
                    } catch (Exception e) {
                        logger.info("Error with reset command for authorId {}, guildId {}, exception {}", authorId, guildId, e.getMessage());
                        return event.editReply("Issue resetting. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb").then();
                    }
                case "ROULETTE":
                    try {
                        long startTime = System.currentTimeMillis();
                        Mono<Void> response = event.deferReply().withEphemeral(true).then(roulette(event, authorId, guildId, guildName, username, 0)).then();
                        long endTime = System.currentTimeMillis();
                        long totalTime = endTime - startTime;
                        logger.info("Finished " + event.getCommandName() + " in " + totalTime + "ms for guild: " + guildName);
                        return response;
                    } catch (Exception e) {
                        logger.info("Error with " + event.getCommandName() + " command for authorId {}, guildId {}, exception {}", authorId, guildId, e.getMessage());
                        return event.editReply("Issue with " + event.getCommandName() + " command. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb").then();
                    }
                case "CLEARPREDICTIONS":
                    try {
                        long startTime = System.currentTimeMillis();
                        Mono<Void> response = event.deferReply().withEphemeral(true).then(clearPredictions(event, authorId, guildId, guildName, username, 0)).then();
                        long endTime = System.currentTimeMillis();
                        long totalTime = endTime - startTime;
                        logger.info("Finished " + event.getCommandName() + " in " + totalTime + "ms for guild: " + guildName);
                        return response;
                    } catch (Exception e) {
                        logger.info("Error with clear predictions command for authorId {}, guildId {}, exception {}", authorId, guildId, e.getMessage());
                        return event.editReply("Issue clearing predictions. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb").then();
                    }
                default:
            }
            return null;
        }).then();

        Mono<Void> buttonCommandHandler = gateway.on(ButtonInteractionEvent.class, event -> {
            String authorId = event.getInteraction().getUser().getId().asString();
            String guildId = event.getInteraction().getGuildId().get().asString();
            String guildName = event.getInteraction().getGuild().block().getName();
            String username = event.getInteraction().getUser().getUsername().replace("-", "");
            if (Constants.OPTION_1_BUTTON.equalsIgnoreCase(event.getCustomId())) {
                List<LayoutComponent> components = new ArrayList<>();
                components.add(ActionRow.of(TextInput.small(Constants.WAGER_MODAL, "Wager").placeholder("enter amount to wager").required(true)));
                return event.presentModal("Wager", Constants.OPTION_1_BUTTON, components);
            } else if (Constants.OPTION_2_BUTTON.equalsIgnoreCase(event.getCustomId())) {
                List<LayoutComponent> components = new ArrayList<>();
                components.add(ActionRow.of(TextInput.small(Constants.WAGER_MODAL, "Wager").placeholder("enter amount to wager").required(true)));
                return event.presentModal("Wager", Constants.OPTION_2_BUTTON, components);
            } else if (Constants.LOCK_BUTTON.equalsIgnoreCase(event.getCustomId())) {
                return event.deferReply().withEphemeral(true).then(lockHandler(event, guildId, username)).then();
            } else if (Constants.REPLAY_BUTTON.equalsIgnoreCase(event.getCustomId())) {
                try {
                    long startTime = System.currentTimeMillis();
                    Mono<Void> response = event.deferReply().withEphemeral(true).then(replay(event, authorId, guildId, guildName, username)).then();
                    long endTime = System.currentTimeMillis();
                    long totalTime = endTime - startTime;
                    logger.info("Finished " + event.getCustomId() + " in " + totalTime + "ms for guild: " + guildName);
                    return response;
                } catch (Exception e) {
                    logger.info("Error with replay command for authorId {}, guildId {}, exception {}", authorId, guildId, e.getMessage());
                    return event.editReply("Issue replaying. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb").then();
                }
            } else if (Constants.REFUND_BUTTON.equalsIgnoreCase(event.getCustomId())) {
                try {
                    long startTime = System.currentTimeMillis();
                    Mono<Void> response = event.deferReply().withEphemeral(true).then(refund(event, authorId, guildId, guildName, username, 0)).then();
                    long endTime = System.currentTimeMillis();
                    long totalTime = endTime - startTime;
                    logger.info("Finished " + event.getCustomId() + " in " + totalTime + "ms for guild: " + guildName);
                    return response;
                } catch (Exception e) {
                    logger.info("Error with refund command for authorId {}, guildId {}, exception {}", authorId, guildId, e.getMessage());
                    return event.editReply("Issue refunding. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb").then();
                }
            }
            return event.reply("Something went wrong.");
        }).then();

        Mono<Void> modalCommandHandler = gateway.on(ModalSubmitInteractionEvent.class, event -> {
            String authorId = event.getInteraction().getUser().getId().asString();
            String guildId = event.getInteraction().getGuildId().get().asString();
            String guildName = event.getInteraction().getGuild().block().getName();
            String username = event.getInteraction().getUser().getUsername().replace("-", "");

            if (Constants.CONFIGURE_MODAL.equalsIgnoreCase(event.getInteraction().getData().data().get().customId().get())) {
                try {
                    long startTime = System.currentTimeMillis();
                    Mono<Void> response = event.deferReply().withEphemeral(true).then(configureHandler(event, authorId, username, guildId, event.getCustomId(), guildName, 0)).then();
                    long endTime = System.currentTimeMillis();
                    long totalTime = endTime - startTime;
                    logger.info("Finished configure handler in " + totalTime + "ms for guild: " + guildName);
                    return response;
                } catch (Exception e) {
                    logger.info("Error configuring for authorId {}, guildId {}", authorId, guildId);
                    logger.info(e.getMessage());
                    return event.editReply("There was an issue handling your command. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb").then();
                }
            } else if (Constants.OPTION_1_BUTTON.equalsIgnoreCase(event.getCustomId()) || Constants.OPTION_2_BUTTON.equalsIgnoreCase(event.getCustomId())) {
                try {
                    long startTime = System.currentTimeMillis();
                    Mono<Void> response = event.deferReply().withEphemeral(true).then(join(event, authorId, username, guildId, event.getCustomId(), guildName, 0)).then();
                    long endTime = System.currentTimeMillis();
                    long totalTime = endTime - startTime;
                    logger.info("Finished joinprediction in " + totalTime + "ms for guild: " + guildName);
                    return response;
                } catch (Exception e) {
                    logger.info("Error joining prediction for authorId {}, guildId {}", authorId, guildId);
                    logger.info(e.getMessage());
                    return event.editReply("There was an issue handling your command. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb").then();
                }
            }
            return event.reply("There was an issue handling your command. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb").withEphemeral(true).then();

        }).then();

        Mono<Void> selectCommandHandler = gateway.on(SelectMenuInteractionEvent.class, event -> {
            String authorId = event.getInteraction().getUser().getId().asString();
            String guildId = event.getInteraction().getGuildId().get().asString();
            String guildName = event.getInteraction().getGuild().block().getName();
            String username = event.getInteraction().getUser().getUsername().replace("-", "");

            Mono<Void> response = null;
            try {
                response = event.deferReply().withEphemeral(true).then(endGamble(event, guildId, authorId, username, guildName, 0)).then();
            } catch (Exception e) {
                logger.info("Error ending prediction for authorId {}, guildId {}", authorId, guildId);
                logger.info(e.getMessage());
            }

            return response;
        }).then();


        return slashCommandHandler.and(buttonCommandHandler).and(modalCommandHandler).and(selectCommandHandler).block();
    }

    private Mono<Message> configureHandler(ModalSubmitInteractionEvent event, String authorId, String username, String
            guildId, String optionNumber, String guildName, int retries) throws Exception {

        try {
            String roleName = event.getComponents(TextInput.class).get(0).getValue().get().trim();
            String dailyPoints = event.getComponents(TextInput.class).get(1).getValue().get();
            String startingPoints = event.getComponents(TextInput.class).get(2).getValue().get();
            String resetSchedule = event.getComponents(TextInput.class).get(3).getValue().get().toUpperCase();


            int dailyPointsInt = Integer.parseInt(dailyPoints);
            int startingPointsInt = Integer.parseInt(startingPoints);

            if (dailyPointsInt < 0 || startingPointsInt < 0) {
                throw new NumberFormatException();
            }

            if (dailyPointsInt > 10000) {
                return event.editReply("Maximum daily points is 10,000.");
            }

            if (startingPointsInt > 100000) {
                return event.editReply("Maximum starting points is 100,000.");
            }

            if (resetSchedule.equalsIgnoreCase(Constants.RESET_SCHEDULES_DAILY) ||
                    resetSchedule.equalsIgnoreCase(Constants.RESET_SCHEDULES_MONTHLY) ||
                    resetSchedule.equalsIgnoreCase(Constants.RESET_SCHEDULES_MO) ||
                    resetSchedule.equalsIgnoreCase(Constants.RESET_SCHEDULES_TU) ||
                    resetSchedule.equalsIgnoreCase(Constants.RESET_SCHEDULES_WE) ||
                    resetSchedule.equalsIgnoreCase(Constants.RESET_SCHEDULES_TH) ||
                    resetSchedule.equalsIgnoreCase(Constants.RESET_SCHEDULES_FR) ||
                    resetSchedule.equalsIgnoreCase(Constants.RESET_SCHEDULES_SAT) ||
                    resetSchedule.equalsIgnoreCase(Constants.RESET_SCHEDULES_SUN) ||
                    StringUtils.isBlank(resetSchedule)) {
                Map<String, AttributeValue> returnedItem = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
                DiscordServer discordServer = new DiscordServer(guildId, returnedItem);
                String previousResetSchedule = discordServer.getResetSchedule();
                if (!resetSchedule.equalsIgnoreCase(previousResetSchedule)) {
                    if (StringUtils.isNotBlank(previousResetSchedule)) {
                        removeResetSchedule(previousResetSchedule, guildId, 0);
                    }
                    if (!StringUtils.isBlank(resetSchedule)) {
                        addResetSchedule(resetSchedule, guildId, 0);
                    }
                }

            } else {
                return event.editReply("Please enter a valid value for reset frequency. Valid values are Daily, Monthly, Mo, Tu, We, Th, Fr, Sat, Sun.");
            }

            HashMap<String, AttributeValue> updatedValues = new HashMap<>();
            updatedValues.put(":" + Constants.ROLE, AttributeValue.builder().s(roleName).build());
            updatedValues.put(":" + Constants.DAILY_POINTS, AttributeValue.builder().s(dailyPoints).build());
            updatedValues.put(":" + Constants.STARTING_POINTS, AttributeValue.builder().s(startingPoints).build());
            updatedValues.put(":" + Constants.RESET_SCHEDULE, AttributeValue.builder().s(resetSchedule).build());
            HashSet<String> keys = new HashSet<>();
            keys.add(Constants.ROLE);
            keys.add(Constants.DAILY_POINTS);
            keys.add(Constants.STARTING_POINTS);
            keys.add(Constants.RESET_SCHEDULE);
            dynamoDBHelper.updateTableItem(guildId, updatedValues, keys);
            return event.editReply("Settings have been saved.");
        } catch (
                NumberFormatException e) {
            return event.editReply("Invalid entry for settings. Ensure you are entering positive integers for daily points and starting points.");
        }
    }

    public void addResetSchedule(String resetScheduleFrequency, String guildId, int retries) throws Exception {
        try {
            Map<String, AttributeValue> itemFromTable = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, resetScheduleFrequency);
            Map<String, AttributeValue> guild_map = new HashMap<>();
            int guildVersion = 0;
            if (itemFromTable != null) {
                if (itemFromTable.get(Constants.GUILDS) != null) {
                    if (itemFromTable.get(Constants.GUILDS).m().containsKey(guildId)) {
                        return;
                    }
                    guild_map = makeModifiableCopyOfMap(itemFromTable.get(Constants.GUILDS));
                }

                if (itemFromTable.get(Constants.GUILDS_VERSION) != null) {
                    guildVersion = Integer.parseInt(itemFromTable.get(Constants.GUILDS_VERSION).n());
                }
            }


            guild_map.put(guildId, AttributeValue.builder().s(guildId).build());
            HashMap<String, AttributeValue> updatedValues = new HashMap<>();
            updatedValues.put(":" + Constants.GUILDS, AttributeValue.builder().m(guild_map).build());
            updatedValues.put(":" + Constants.GUILDS_VERSION, AttributeValue.builder().n(String.valueOf(guildVersion + 1)).build());
            HashSet<String> keys = new HashSet<>();
            keys.add(Constants.GUILDS);
            keys.add(Constants.GUILDS_VERSION);
            dynamoDBHelper.updateTableItem(resetScheduleFrequency, updatedValues, keys);

        } catch (ConditionalCheckFailedException e) {
            logger.info(guildId + ": " + e.getMessage());
            if (retries < 10) {
                addResetSchedule(resetScheduleFrequency, guildId, retries + 1);
            } else {
                throw new Exception("Reached max retries for adding reset schedules. guildId: " + guildId);
            }
        } catch (Exception e) {
            throw e;
        }

    }

    public void removeResetSchedule(String previousResetScheduleFrequency, String guildId, int retries) throws Exception {
        try {
            Map<String, AttributeValue> itemFromTable = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, previousResetScheduleFrequency);
            Map<String, AttributeValue> guild_map = new HashMap<>();
            int guildVersion = 0;
            if (itemFromTable != null) {
                if (itemFromTable.get(Constants.GUILDS) != null) {
                    if (!itemFromTable.get(Constants.GUILDS).m().containsKey(guildId)) {
                        return;
                    }
                    guild_map = makeModifiableCopyOfMap(itemFromTable.get(Constants.GUILDS));
                }

                if (itemFromTable.get(Constants.GUILDS_VERSION) != null) {
                    guildVersion = Integer.parseInt(itemFromTable.get(Constants.GUILDS_VERSION).n());
                }
            }


            guild_map.remove(guildId, AttributeValue.builder().s(guildId).build());
            HashMap<String, AttributeValue> updatedValues = new HashMap<>();
            updatedValues.put(":" + Constants.GUILDS, AttributeValue.builder().m(guild_map).build());
            updatedValues.put(":" + Constants.GUILDS_VERSION, AttributeValue.builder().n(String.valueOf(guildVersion + 1)).build());
            HashSet<String> keys = new HashSet<>();
            keys.add(Constants.GUILDS);
            keys.add(Constants.GUILDS_VERSION);
            dynamoDBHelper.updateTableItem(previousResetScheduleFrequency, updatedValues, keys);

        } catch (ConditionalCheckFailedException e) {
            logger.info(guildId + ": " + e.getMessage());
            if (retries < 10) {
                addResetSchedule(previousResetScheduleFrequency, guildId, retries + 1);
            } else {
                throw new Exception("Reached max retries for adding reset schedules. guildId: " + guildId);
            }
        } catch (Exception e) {
            throw e;
        }
    }

    private HashMap<String, AttributeValue> makeModifiableCopyOfMap(AttributeValue attributeValue) {
        Map<String, AttributeValue> original = attributeValue.m();
        HashMap<String, AttributeValue> copy = new HashMap<>();

        for (Map.Entry<String, AttributeValue> entry : original.entrySet()) {
            copy.put(entry.getKey(), entry.getValue().toBuilder().build());
        }
        return copy;
    }

    private Mono<Void> lockHandler(ButtonInteractionEvent event, String guildId, String username) {
        Map<String, AttributeValue> returnedItem = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
        DiscordServer discordServer = new DiscordServer(guildId, returnedItem);

        if (!StringUtils.isBlank(discordServer.getRole()) && !Constants.EVERYONE.equalsIgnoreCase(discordServer.getRole())) {
            boolean hasRole = false;
            for (Role role : event.getInteraction().getMember().get().getRoles().toIterable()) {
                if (discordServer.getRole().equalsIgnoreCase(role.getName())) {
                    hasRole = true;
                }
            }
            if (!hasRole) {
                return event.editReply("Only users with role (" + discordServer.getRole() + ") can modify a prediction session.").then();
            }
        }
        String messageId = event.getInteraction().getData().message().get().id().asString();
        RestMessage message = client.getMessageById(event.getInteraction().getChannelId(), Snowflake.of(messageId));
        Possible<List<ComponentData>> components = message.getData().block().components();
        String gambleTitle = event.getInteraction().getData().message().get().embeds().get(0).title().get();
        String gambleOption1 = event.getInteraction().getData().message().get().embeds().get(0).fields().get().get(1).name();
        String gamblePot1 = event.getInteraction().getData().message().get().embeds().get(0).fields().get().get(1).value();
        String gambleOption2 = event.getInteraction().getData().message().get().embeds().get(0).fields().get().get(2).name();
        String gamblePot2 = event.getInteraction().getData().message().get().embeds().get(0).fields().get().get(2).value();


        if (Constants.OPTION_1_BUTTON.equalsIgnoreCase(components.get().get(0).components().get().get(0).customId().get())) {
            Button button3 = Button.success(Constants.LOCK_BUTTON, "Unlock");
            List<Button> buttons = new ArrayList<>();
            buttons.add(button3);

            SelectMenu selectMenu = SelectMenu.of("OUTCOME",
                    SelectMenu.Option.of(gambleOption1, gambleOption1),
                    SelectMenu.Option.of(gambleOption2, gambleOption2),
                    SelectMenu.Option.of("Cancel Prediction", Constants.CANCEL)
            ).withPlaceholder("Select Outcome To End Prediction");

            EmbedData embed = EmbedData.builder()
                    .color(1)
                    .title(gambleTitle)
                    .addField(EmbedFieldData.builder().name("Options:").value("Pots:").inline(true).build())
                    .addField(EmbedFieldData.builder().name(gambleOption1).value(gamblePot1).inline(true).build())
                    .addField(EmbedFieldData.builder().name(gambleOption2).value(gamblePot2).inline(true).build())
                    .addField(EmbedFieldData.builder().name("").value("Use the buttons below to join, lock or cancel the prediction.").inline(false).build())
                    .footer(EmbedFooterData.builder().text("Locked by " + username).build())
                    .timestamp(String.valueOf(Instant.now()))
                    .build();

            message.edit(MessageEditRequest.builder().addEmbed(embed).addComponent(ActionRow.of(selectMenu).getData()).addComponent(ActionRow.of(buttons).getData()).build()).subscribe();
            return event.deleteReply();
        } else {
            Button button = Button.primary(Constants.OPTION_1_BUTTON, gambleOption1);
            Button button2 = Button.primary(Constants.OPTION_2_BUTTON, gambleOption2);
            Button button3 = Button.success(Constants.LOCK_BUTTON, "Lock");
            List<Button> buttons = new ArrayList<>();
            buttons.add(button);
            buttons.add(button2);
            buttons.add(button3);

            EmbedData embed = EmbedData.builder()
                    .color(1)
                    .title(gambleTitle)
                    .addField(EmbedFieldData.builder().name("Options:").value("Pots:").inline(true).build())
                    .addField(EmbedFieldData.builder().name(gambleOption1).value(gamblePot1).inline(true).build())
                    .addField(EmbedFieldData.builder().name(gambleOption2).value(gamblePot2).inline(true).build())
                    .addField(EmbedFieldData.builder().name("").value("Use the buttons below to join, lock or cancel the prediction.").inline(false).build())
                    .footer(EmbedFooterData.builder().text("Unlocked by " + username).build())
                    .timestamp(String.valueOf(Instant.now()))
                    .build();

            message.edit(MessageEditRequest.builder().addEmbed(embed).addComponent(ActionRow.of(buttons).getData()).build()).subscribe();
            return event.deleteReply();
        }
    }

    private Mono<Message> clearPredictions(ChatInputInteractionEvent event, String authorId, String guildId, String guildName
            , String username, int retries) throws Exception {
        try {
            Map<String, AttributeValue> returnedItem = this.dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
            DiscordServer discordServer = new DiscordServer(guildId, returnedItem);

            if (!StringUtils.isBlank(discordServer.getRole()) && !Constants.EVERYONE.equalsIgnoreCase(discordServer.getRole())) {
                boolean hasRole = false;
                for (Role role : event.getInteraction().getMember().get().getRoles().toIterable()) {
                    if (discordServer.getRole().equalsIgnoreCase(role.getName())) {
                        hasRole = true;
                    }
                }
                if (!hasRole) {
                    return event.editReply("Only users with role (" + discordServer.getRole() + ") can end a prediction session.");
                }
            }
            if (discordServer.isGambleOneInProgress() || discordServer.isGambleTwoInProgress() || discordServer.isGambleThreeInProgress()) {
                endAllGambleSessions(guildId, discordServer);
            } else {
                return event.editReply("No sessions to clear..");
            }
            return event.editReply("All prediction sessions cleared.");
        } catch (ConditionalCheckFailedException e) {
            logger.info(guildId + ": " + e.getMessage());
            if (retries < 10) {
                return clearPredictions(event, authorId, guildId, guildName, username, retries + 1);
            } else {
                throw (e);
            }

        } catch (Exception e) {
            logger.info("Error with clear prediction command for authorId {}, guildId {}, exception {}", authorId, guildId, e.getMessage());
            return event.editReply("Issue clearing prediction. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb");
        }
    }

    private void endAllGambleSessions(String guildId, DiscordServer discordServer) throws Exception {
        Map<String, AttributeValue> modifiableUserMap = discordServer.getModifiableUsers();
        HashMap<String, AttributeValue> mapToPut = new HashMap<>();
        HashSet<String> keys = new HashSet<>();


        if (discordServer.isGambleOneInProgress()) {
            refundPrediction(discordServer, discordServer.getGamblersOne(), modifiableUserMap);
            mapToPut.put(":" + Constants.GAMBLERS_ONE, AttributeValue.builder().m(new HashMap<>()).build());
            mapToPut.put(":" + Constants.IN_PROGRESS_GAMBLE_DATA_ONE, AttributeValue.builder().s(Constants.NO_SESSION).build());
            mapToPut.put(":" + Constants.GAMBLERS_ONE_VERSION, AttributeValue.builder().n(String.valueOf(discordServer.getGamblersOneVersion() + 1)).build());
            mapToPut.put(":" + Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_ONE, AttributeValue.builder().n(String.valueOf(discordServer.getInProgressGambleOneVersion() + 1)).build());
            keys.add(Constants.IN_PROGRESS_GAMBLE_DATA_ONE);
            keys.add(Constants.GAMBLERS_ONE);
            keys.add(Constants.GAMBLERS_ONE_VERSION);
            keys.add(Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_ONE);

        }
        if (discordServer.isGambleTwoInProgress()) {
            refundPrediction(discordServer, discordServer.getGamblersTwo(), modifiableUserMap);
            mapToPut.put(":" + Constants.GAMBLERS_TWO, AttributeValue.builder().m(new HashMap<>()).build());
            mapToPut.put(":" + Constants.IN_PROGRESS_GAMBLE_DATA_TWO, AttributeValue.builder().s(Constants.NO_SESSION).build());
            mapToPut.put(":" + Constants.GAMBLERS_TWO_VERSION, AttributeValue.builder().n(String.valueOf(discordServer.getGamblersTwoVersion() + 1)).build());
            mapToPut.put(":" + Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_TWO, AttributeValue.builder().n(String.valueOf(discordServer.getInProgressGambleTwoVersion() + 1)).build());
            keys.add(Constants.IN_PROGRESS_GAMBLE_DATA_TWO);
            keys.add(Constants.GAMBLERS_TWO);
            keys.add(Constants.GAMBLERS_TWO_VERSION);
            keys.add(Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_TWO);
        }
        if (discordServer.isGambleThreeInProgress()) {
            refundPrediction(discordServer, discordServer.getGamblersThree(), modifiableUserMap);
            mapToPut.put(":" + Constants.GAMBLERS_THREE, AttributeValue.builder().m(new HashMap<>()).build());
            mapToPut.put(":" + Constants.IN_PROGRESS_GAMBLE_DATA_THREE, AttributeValue.builder().s(Constants.NO_SESSION).build());
            mapToPut.put(":" + Constants.GAMBLERS_THREE_VERSION, AttributeValue.builder().n(String.valueOf(discordServer.getGamblersThreeVersion() + 1)).build());
            mapToPut.put(":" + Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_THREE, AttributeValue.builder().n(String.valueOf(discordServer.getInProgressGambleThreeVersion() + 1)).build());
            keys.add(Constants.IN_PROGRESS_GAMBLE_DATA_THREE);
            keys.add(Constants.GAMBLERS_THREE);
            keys.add(Constants.GAMBLERS_THREE_VERSION);
            keys.add(Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_THREE);
        }

        mapToPut.put(":" + Constants.USERS, AttributeValue.builder().m(modifiableUserMap).build());
        mapToPut.put(":" + Constants.USERS_VERSION, AttributeValue.builder().n(String.valueOf(discordServer.getUsersVersion() + 1)).build());
        keys.add(Constants.USERS);
        keys.add(Constants.USERS_VERSION);
        dynamoDBHelper.updateTableItem(guildId, mapToPut, keys);

    }

    private static void refundPrediction(DiscordServer discordServer, Map<String, AttributeValue> gamblers, Map<String, AttributeValue> modifiableUserMap) throws Exception {
        if (gamblers != null) {
            for (Map.Entry<String, AttributeValue> gambler : gamblers.entrySet()) {
                int currentPoints = Integer.parseInt(discordServer.getCurrentPoints(gambler.getKey()));
                int pointsBet = Integer.parseInt(gambler.getValue().s().split("-")[Constants.POINTS_BET]);
                String[] userData = modifiableUserMap.get(gambler.getKey()).s().split("-");
                int newUserPoints = currentPoints + pointsBet;
                userData[Constants.CURRENT_POINTS] = String.valueOf(newUserPoints);
                modifiableUserMap.put(gambler.getKey(), AttributeValue.builder().s(userData[Constants.CURRENT_POINTS] +
                        "-" + userData[Constants.USERNAME] + "-" + userData[Constants.DAILY]).build());

            }
        }
    }

    private Mono<Message> replay(DeferrableInteractionEvent event, String authorId, String guildId, String guildName
            , String username) throws Exception {
        Map<String, AttributeValue> returnedItem = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
        DiscordServer discordServer = new DiscordServer(guildId, returnedItem);
        if (!discordServer.hasServerBeenInitialized()) {
            try {
                long startTime = System.currentTimeMillis();
                initializeGambler(event, authorId, username, guildId, guildName, 0);
                long endTime = System.currentTimeMillis();
                long totalTime = endTime - startTime;
                returnedItem = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
                discordServer = new DiscordServer(guildId, returnedItem);
                logger.info("Finished initialize player in " + totalTime + "ms for guild: " + guildName);
            } catch (Exception e) {
                logger.info("Error initializing player for authorId {}, guildId {}", authorId, guildId);
                logger.info(e.getMessage());
                throw e;
            }
        }

        if (!StringUtils.isBlank(discordServer.getRole()) && !Constants.EVERYONE.equalsIgnoreCase(discordServer.getRole())) {
            boolean hasRole = false;
            for (Role role : event.getInteraction().getMember().get().getRoles().toIterable()) {
                if (discordServer.getRole().equalsIgnoreCase(role.getName())) {
                    hasRole = true;
                }
            }
            if (!hasRole) {
                return event.editReply("Only users with role (" + discordServer.getRole() + ") can replay.");
            }
        }
        String previousGambleTitle = event.getInteraction().getData().message().get().embeds().get(0).title().get().replace("~", "");
        String previousGambleOption1 = event.getInteraction().getData().message().get().embeds().get(0).fields().get().get(1).name();
        String previousGambleOption2 = event.getInteraction().getData().message().get().embeds().get(0).fields().get().get(2).name();

        return startGamble(event, guildId, authorId, username, guildName, previousGambleTitle, previousGambleOption1, previousGambleOption2);
    }

    private Mono<Void> refund(DeferrableInteractionEvent event, String authorId, String guildId, String guildName
            , String authorUsername, int retries) throws Exception {
        try {
            Map<String, AttributeValue> returnedItem = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
            DiscordServer discordServer = new DiscordServer(guildId, returnedItem);
            if (!discordServer.hasServerBeenInitialized()) {
                try {
                    long startTime = System.currentTimeMillis();
                    initializeGambler(event, authorId, authorUsername, guildId, guildName, 0);
                    long endTime = System.currentTimeMillis();
                    long totalTime = endTime - startTime;
                    returnedItem = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
                    discordServer = new DiscordServer(guildId, returnedItem);
                    logger.info("Finished initialize player in " + totalTime + "ms for guild: " + guildName);
                } catch (Exception e) {
                    logger.info("Error initializing player for authorId {}, guildId {}", authorId, guildId);
                    logger.info(e.getMessage());
                    throw e;
                }
            }

            if (!StringUtils.isBlank(discordServer.getRole()) && !Constants.EVERYONE.equalsIgnoreCase(discordServer.getRole())) {
                boolean hasRole = false;
                for (Role role : event.getInteraction().getMember().get().getRoles().toIterable()) {
                    if (discordServer.getRole().equalsIgnoreCase(role.getName())) {
                        hasRole = true;
                    }
                }
                if (!hasRole) {
                    return event.editReply("Only users with role (" + discordServer.getRole() + ") can refund.").then();
                }
            }
            String messageId = event.getInteraction().getData().message().get().id().asString();

            String previousPredictionKey;
            String previousGamblersKey;
            Map<String, AttributeValue> previousGamblers;
            Map<String, AttributeValue> users;
            String title;
            String option1;
            String option2;
            String option1Pot;
            String option2Pot;

            if (discordServer.getPreviousPredictionOne() != null && messageId.equalsIgnoreCase(discordServer.getPreviousPredictionOne().getMessageId())) {
                previousPredictionKey = Constants.PREVIOUS_PREDICTION_ONE;
                previousGamblers = discordServer.getPreviousGamblersOne();
                previousGamblersKey = Constants.PREVIOUS_GAMBLERS_ONE;
                title = discordServer.getPreviousPredictionOne().getTitle();
                option1 = discordServer.getPreviousPredictionOne().getOption1();
                option2 = discordServer.getPreviousPredictionOne().getOption2();
                option1Pot = discordServer.getPreviousPredictionOne().getOption1Pot();
                option2Pot = discordServer.getPreviousPredictionOne().getOption2Pot();

            } else if (discordServer.getPreviousPredictionTwo() != null && messageId.equalsIgnoreCase(discordServer.getPreviousPredictionTwo().getMessageId())) {
                previousPredictionKey = Constants.PREVIOUS_PREDICTION_TWO;
                previousGamblersKey = Constants.PREVIOUS_GAMBLERS_TWO;
                previousGamblers = discordServer.getGamblersTwo();
                title = discordServer.getPreviousPredictionTwo().getTitle();
                option1 = discordServer.getPreviousPredictionTwo().getOption1();
                option2 = discordServer.getPreviousPredictionTwo().getOption2();
                option1Pot = discordServer.getPreviousPredictionTwo().getOption1Pot();
                option2Pot = discordServer.getPreviousPredictionTwo().getOption2Pot();
            } else if (discordServer.getPreviousPredictionThree() != null && messageId.equalsIgnoreCase(discordServer.getPreviousPredictionThree().getMessageId())) {
                previousPredictionKey = Constants.PREVIOUS_PREDICTION_THREE;
                previousGamblers = discordServer.getGamblersThree();
                previousGamblersKey = Constants.PREVIOUS_GAMBLERS_THREE;
                title = discordServer.getPreviousPredictionThree().getTitle();
                option1 = discordServer.getPreviousPredictionThree().getOption1();
                option2 = discordServer.getPreviousPredictionThree().getOption2();
                option1Pot = discordServer.getPreviousPredictionThree().getOption1Pot();
                option2Pot = discordServer.getPreviousPredictionThree().getOption2Pot();
            } else {
                return event.editReply("Prediction not found. You can only refund the last three predictions.").then();
            }

            Map<String, AttributeValue> modifiableUserMap = discordServer.getModifiableUsers();
            for (Map.Entry<String, AttributeValue> gambler : previousGamblers.entrySet()) {
                int currentPoints = Integer.parseInt(discordServer.getCurrentPoints(gambler.getKey()));
                int pointsDelta = Integer.parseInt(gambler.getValue().s().split("-")[Constants.POINTS_BET]);
                String[] userData = modifiableUserMap.get(gambler.getKey()).s().split("-");
                String newUserPoints;
                if ("WIN".equalsIgnoreCase(gambler.getValue().s().split("-")[0])) {
                    newUserPoints = String.valueOf(currentPoints - pointsDelta);
                } else {
                    newUserPoints = String.valueOf(currentPoints + pointsDelta);
                }
                String username = discordServer.getUsername(gambler.getKey());
                modifiableUserMap.put(gambler.getKey(), AttributeValue.builder().s(newUserPoints + "-" + username + "-" + userData[Constants.DAILY]).build());
            }

            refundGambleSession(guildId, modifiableUserMap, discordServer, previousGamblersKey, previousPredictionKey);

            EmbedData embed = EmbedData.builder()
                    .color(1)
                    .title(title)
                    .addField(EmbedFieldData.builder().name("Options:").value("Pots:").inline(true).build())
                    .addField(EmbedFieldData.builder().name(option1).value(option1Pot).inline(true).build())
                    .addField(EmbedFieldData.builder().name(option2).value(option2Pot).inline(true).build())
                    .addField(EmbedFieldData.builder().name("").value("REFUNDED").inline(false).build())
                    .timestamp(String.valueOf(Instant.now()))
                    .footer(EmbedFooterData.builder().text("Refunded").build())
                    .build();
            Button button3 = Button.success(Constants.LOCK_BUTTON, "Unlock");
            button3 = button3.disabled();
            List<Button> buttons = new ArrayList<>();
            buttons.add(button3);
            Button replay_button = Button.success(Constants.REPLAY_BUTTON, "Replay");
            buttons.add(replay_button);
            Button refund_button = Button.danger(Constants.REFUND_BUTTON, "Refund");
            refund_button = refund_button.disabled();
            buttons.add(refund_button);

            SelectMenu selectMenu = SelectMenu.of("OUTCOME",
                    SelectMenu.Option.of(option1, option1),
                    SelectMenu.Option.of(option2, option2),
                    SelectMenu.Option.of("Cancel Prediction", Constants.CANCEL)
            ).withPlaceholder("Select Outcome To End Prediction");
            selectMenu = selectMenu.disabled();
            RestMessage message = client.getMessageById(event.getInteraction().getChannelId(), Snowflake.of(messageId));
            message.edit(MessageEditRequest.builder().addComponent(ActionRow.of(selectMenu).getData()).addEmbed(embed).addComponent(ActionRow.of(buttons).getData()).build()).subscribe();


            return event.editReply("Points have been refunded.").then();
        } catch (ConditionalCheckFailedException e) {
            logger.info(guildId + ": " + e.getMessage());
            if (retries < 10) {
                return refund(event, authorId, guildId, guildName, authorUsername, retries + 1);
            } else {
                throw (e);
            }

        } catch (Exception e) {
            logger.info("Error refunding prediction for authorId {}, guildId {}", authorId, guildId);
            logger.info(e.getMessage());
            return event.editReply().withContent("Issue refunding prediction. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb").then();
        }
    }

    private Mono<Void> roulette(ChatInputInteractionEvent event, String authorId, String guildId, String guildName, String username, int retries) throws Exception {
        Map<String, AttributeValue> returnedItem = this.dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
        DiscordServer discordServer = new DiscordServer(guildId, returnedItem);

        if (!discordServer.hasServerBeenInitialized() || !discordServer.userExists(authorId)) {
            try {
                long startTime = System.currentTimeMillis();
                initializeGambler(event, authorId, username, guildId, guildName, 0);
                long endTime = System.currentTimeMillis();
                long totalTime = endTime - startTime;
                logger.info("Finished initialize player in " + totalTime + "ms for guild: " + guildName);

            } catch (Exception e) {
                logger.info("Error initializing player for authorId {}, guildId {}", authorId, guildId);
                logger.info(e.getMessage());
                throw e;
            }
        }
        int wager = 0;
        try {
            wager = Integer.parseInt(event.getOption("wager").get().getValue().get().asString());
        } catch (NumberFormatException e) {
            return event.editReply("Must wager a valid amount.").then();
        }
        if (Integer.parseInt(discordServer.getCurrentPoints(authorId)) < wager) {
            return event.editReply("Cannot wager more points than you have.").then();
        } else if (wager < 1) {
            return event.editReply("Must wager a valid amount.").then();
        } else {
            String result = "";
            Double randomNum = Math.random();
            if (randomNum < .474) {
                result = result.concat(username + " won " + wager + " points in roulette!");
            } else {
                result = result.concat(username + " lost " + wager + " points in roulette. Better luck next time!");
                wager = wager * -1;
            }
            String newPoints;
            Map<String, AttributeValue> newUserMapUpdate = discordServer.getModifiableUsers();
            if (Long.parseLong(discordServer.getCurrentPoints(authorId)) + wager > Integer.MAX_VALUE) {
                newPoints = String.valueOf(Integer.MAX_VALUE);
            } else {
                newPoints = String.valueOf(Integer.parseInt(discordServer.getCurrentPoints(authorId)) + wager);
            }
            String lastDailyPoints = newUserMapUpdate.get(authorId).s().split("-")[Constants.DAILY];
            newUserMapUpdate.put(authorId, AttributeValue.builder().s(newPoints + "-" + username + "-" + lastDailyPoints).build());
            HashMap<String, AttributeValue> updatedValues = new HashMap<>();
            updatedValues.put(":" + Constants.USERS, AttributeValue.builder().m(newUserMapUpdate).build());
            updatedValues.put(":" + Constants.USERS_VERSION, AttributeValue.builder().n(String.valueOf(discordServer.getUsersVersion() + 1)).build());
            HashSet<String> keys = new HashSet<>();
            keys.add(Constants.USERS);
            keys.add(Constants.USERS_VERSION);
            try {
                dynamoDBHelper.updateTableItem(guildId, updatedValues, keys);
            } catch (ConditionalCheckFailedException e) {
                if (retries < 10) {
                    roulette(event, guildName, guildId, authorId, username, retries + 1);
                } else {
                    throw (e);
                }
            }
            try {
                MessageCreateRequest messageCreateRequest = MessageCreateRequest.builder().content(result).build();
                client.getChannelById(event.getInteraction().getChannelId()).createMessage(messageCreateRequest).block();
            } catch (Exception e) {
                if (e.getMessage() != null && (e.getMessage().contains("50013") || e.getMessage().contains("50001"))) {
                    return event.editReply("I need permission to send messages in this channel.").then();
                }
            }
            return event.deleteReply();

        }


    }


    private Mono<Void> displayConfigureModal(ChatInputInteractionEvent event, String authorId, String guildId, String guildName, String username) throws Exception {
        Map<String, AttributeValue> returnedItem = this.dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
        DiscordServer discordServer = new DiscordServer(guildId, returnedItem);

        if (!discordServer.hasServerBeenInitialized()) {
            try {
                long startTime = System.currentTimeMillis();
                initializeGambler(event, authorId, username, guildId, guildName, 0);
                long endTime = System.currentTimeMillis();
                long totalTime = endTime - startTime;
                logger.info("Finished initialize player in " + totalTime + "ms for guild: " + guildName);

            } catch (Exception e) {
                logger.info("Error initializing player for authorId {}, guildId {}", authorId, guildId);
                logger.info(e.getMessage());
                throw e;
            }
        }


        if (authorId.equalsIgnoreCase(event.getInteraction().getGuild().block().getOwnerId().asString())) {
            List<LayoutComponent> components = new ArrayList<>();
            String role = discordServer.getRole();
            String startingPoints = discordServer.getStartingPoints();
            String dailyPoints = discordServer.getDailyPoints();
            String resetSchedule = discordServer.getResetSchedule();

            if (!StringUtils.isBlank(role)) {
                components.add(ActionRow.of(TextInput.small(Constants.ROLE_MODAL, "Role").prefilled(discordServer.getRole()).required(false)));
            } else {
                components.add(ActionRow.of(TextInput.small(Constants.ROLE_MODAL, "Role Name").placeholder("Role to limit who can start, end and refund predictions.").required(false)));
            }
            if (dailyPoints != null) {
                components.add(ActionRow.of(TextInput.small(Constants.DAILY_MODAL, "Daily Points Amount").prefilled(dailyPoints).required(true)));
            } else {
                components.add(ActionRow.of(TextInput.small(Constants.DAILY_MODAL, "Daily Points Amount").prefilled("100").required(true)));
            }
            if (startingPoints != null) {
                components.add(ActionRow.of(TextInput.small(Constants.STARTING_POINTS_MODAL, "Starting Points Amount").prefilled(startingPoints).required(true)));
            } else {
                components.add(ActionRow.of(TextInput.small(Constants.STARTING_POINTS_MODAL, "Starting Points Amount").prefilled("1000").required(true)));
            }

            if (resetSchedule != null) {
                components.add(ActionRow.of(TextInput.small(Constants.RESET_SCHEDULE_MODAL, "Reset Frequency").prefilled(resetSchedule).required(false)));
            } else {
                components.add(ActionRow.of(TextInput.small(Constants.RESET_SCHEDULE_MODAL, "Reset Frequency").placeholder("Daily, Monthly, Mo, Tu, We, Th, Fr, Sat, Sun").required(false)));
            }
            return event.presentModal("Configurations", Constants.CONFIGURE_MODAL, components);
        } else {
            return event.reply("Only the server owner can configure settings.").withEphemeral(true).then();
        }

    }

    private Mono<Void> reset(ChatInputInteractionEvent event, String authorId, String guildId, String guildName, String username, int retries) throws Exception {
        try {
            Map<String, AttributeValue> returnedItem = this.dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
            DiscordServer discordServer = new DiscordServer(guildId, returnedItem);
            String startingPoints = "1000";
            if (discordServer.getStartingPoints() != null) {
                startingPoints = discordServer.getStartingPoints();
            }

            if (!discordServer.hasServerBeenInitialized()) {
                try {
                    long startTime = System.currentTimeMillis();
                    initializeGambler(event, authorId, username, guildId, guildName, 0);
                    long endTime = System.currentTimeMillis();
                    long totalTime = endTime - startTime;
                    logger.info("Finished initialize player in " + totalTime + "ms for guild: " + guildName);

                } catch (Exception e) {
                    logger.info("Error initializing player for authorId {}, guildId {}", authorId, guildId);
                    logger.info(e.getMessage());
                    throw e;
                }
                return event.editReply("No points to reset.").then();
            }


            if (authorId.equalsIgnoreCase(event.getInteraction().getGuild().block().getOwnerId().asString())) {
                resetPoints(guildId, discordServer.getModifiableUsers(), discordServer, startingPoints);
                return event.editReply("All users points have been reset.").then();
            } else {
                return event.editReply("Only the server owner can reset points.").then();
            }
        } catch (ConditionalCheckFailedException e) {
            logger.info(guildId + ": " + e.getMessage());
            if (retries < 10) {
                return reset(event, authorId, guildId, guildName, username, retries + 1);
            } else {
                throw (e);
            }

        } catch (Exception e) {
            logger.info("Error resetting for authorId {}, guildId {}", authorId, guildId);
            logger.info(e.getMessage());
            return event.editReply().withContent("Issue resetting. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb").then();
        }
    }

    private Mono<Void> awardPoints(ChatInputInteractionEvent event, String authorId, String guildId, String guildName, String username, int retries) throws Exception {
        try {

            Map<String, AttributeValue> returnedItem = this.dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
            DiscordServer discordServer = new DiscordServer(guildId, returnedItem);

            if (!discordServer.hasServerBeenInitialized()) {
                try {
                    long startTime = System.currentTimeMillis();
                    initializeGambler(event, authorId, username, guildId, guildName, 0);
                    long endTime = System.currentTimeMillis();
                    long totalTime = endTime - startTime;
                    logger.info("Finished initialize player in " + totalTime + "ms for guild: " + guildName);

                } catch (Exception e) {
                    logger.info("Error initializing player for authorId {}, guildId {}", authorId, guildId);
                    logger.info(e.getMessage());
                    throw e;
                }
            }


            if (authorId.equalsIgnoreCase(event.getInteraction().getGuild().block().getOwnerId().asString())) {

                discord4j.core.object.entity.User user = event.getOption("user").get().getValue().get().asUser().block();

                if (user != null && discordServer.getUsers().containsKey(user.getId().asString())) {
                    //award points
                    Map<String, AttributeValue> modifiableUsers = discordServer.getModifiableUsers();
                    String[] userData = modifiableUsers.get(user.getId().asString()).s().split("-");
                    int pointsGiven = 0;
                    try {
                        pointsGiven = Integer.parseInt(event.getOption("amount").get().getValue().get().asString());
                        if (pointsGiven <= 0) {
                            throw new NumberFormatException();
                        }
                    } catch (NumberFormatException e) {
                        return event.editReply("Invalid amount.").then();
                    }
                    int newUserPoints = Integer.parseInt(userData[Constants.CURRENT_POINTS]) + pointsGiven;
                    modifiableUsers.put(user.getId().asString(), AttributeValue.builder().s(newUserPoints + "-" + userData[Constants.USERNAME] + "-" + userData[Constants.DAILY]).build());

                    HashMap<String, AttributeValue> updatedValues = new HashMap<>();
                    updatedValues.put(":" + Constants.USERS, AttributeValue.builder().m(modifiableUsers).build());
                    updatedValues.put(":" + Constants.USERS_VERSION, AttributeValue.builder().n(String.valueOf(discordServer.getUsersVersion() + 1)).build());
                    HashSet<String> keys = new HashSet<>();
                    keys.add(Constants.USERS);
                    keys.add(Constants.USERS_VERSION);
                    dynamoDBHelper.updateTableItem(guildId, updatedValues, keys);
                    return event.editReply(pointsGiven + " points have been awarded to " + user.getUsername()).then();
                } else {
                    return event.editReply("User has not yet been initiated. They must run a / command to be initiated with Prediction Bot.").then();
                }
            } else {
                return event.editReply("Only the server owner can award points.").then();
            }
        } catch (ConditionalCheckFailedException e) {
            logger.info(guildId + ": " + e.getMessage());
            if (retries < 10) {
                return awardPoints(event, authorId, guildId, guildName, username, retries + 1);
            } else {
                throw (e);
            }

        } catch (Exception e) {
            logger.info("Error awarding points for authorId {}, guildId {}", authorId, guildId);
            logger.info(e.getMessage());
            return event.editReply().withContent("Issue awarding points. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb").then();
        }
    }

    private Mono<Void> removePoints(ChatInputInteractionEvent event, String authorId, String guildId, String guildName, String username, int retries) throws Exception {
        try {

            Map<String, AttributeValue> returnedItem = this.dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
            DiscordServer discordServer = new DiscordServer(guildId, returnedItem);

            if (!discordServer.hasServerBeenInitialized()) {
                try {
                    long startTime = System.currentTimeMillis();
                    initializeGambler(event, authorId, username, guildId, guildName, 0);
                    long endTime = System.currentTimeMillis();
                    long totalTime = endTime - startTime;
                    logger.info("Finished initialize player in " + totalTime + "ms for guild: " + guildName);

                } catch (Exception e) {
                    logger.info("Error initializing player for authorId {}, guildId {}", authorId, guildId);
                    logger.info(e.getMessage());
                    throw e;
                }
            }


            if (authorId.equalsIgnoreCase(event.getInteraction().getGuild().block().getOwnerId().asString())) {

                discord4j.core.object.entity.User user = event.getOption("user").get().getValue().get().asUser().block();

                if (user != null && discordServer.getUsers().containsKey(user.getId().asString())) {
                    //remove points
                    Map<String, AttributeValue> modifiableUsers = discordServer.getModifiableUsers();
                    String[] userData = modifiableUsers.get(user.getId().asString()).s().split("-");
                    int pointsRemoved = 0;
                    try {
                        pointsRemoved = Integer.parseInt(event.getOption("amount").get().getValue().get().asString());
                        if (pointsRemoved <= 0) {
                            throw new NumberFormatException();
                        }
                    } catch (NumberFormatException e) {
                        return event.editReply("Invalid amount.").then();
                    }
                    int newUserPoints = Integer.parseInt(userData[Constants.CURRENT_POINTS]) - pointsRemoved;
                    if (newUserPoints < 0) {
                        return event.editReply("Cannot make users go into the negative.").then();
                    }
                    modifiableUsers.put(user.getId().asString(), AttributeValue.builder().s(newUserPoints + "-" + userData[Constants.USERNAME] + "-" + userData[Constants.DAILY]).build());

                    HashMap<String, AttributeValue> updatedValues = new HashMap<>();
                    updatedValues.put(":" + Constants.USERS, AttributeValue.builder().m(modifiableUsers).build());
                    updatedValues.put(":" + Constants.USERS_VERSION, AttributeValue.builder().n(String.valueOf(discordServer.getUsersVersion() + 1)).build());
                    HashSet<String> keys = new HashSet<>();
                    keys.add(Constants.USERS);
                    keys.add(Constants.USERS_VERSION);
                    dynamoDBHelper.updateTableItem(guildId, updatedValues, keys);
                    return event.editReply(pointsRemoved + " points have been removed from " + user.getUsername()).then();
                } else {
                    return event.editReply("User has not yet been initiated. They must run a / command to be initiated with Prediction Bot.").then();
                }
            } else {
                return event.editReply("Only the server owner can remove points.").then();
            }
        } catch (ConditionalCheckFailedException e) {
            logger.info(guildId + ": " + e.getMessage());
            if (retries < 10) {
                return removePoints(event, authorId, guildId, guildName, username, retries + 1);
            } else {
                throw (e);
            }

        } catch (Exception e) {
            logger.info("Error removing points for authorId {}, guildId {}", authorId, guildId);
            logger.info(e.getMessage());
            return event.editReply().withContent("Issue removing points. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb").then();
        }
    }

    private Mono<Void> removeUser(ChatInputInteractionEvent event, String authorId, String guildId, String guildName, String username, int retries) throws Exception {
        try {
            Map<String, AttributeValue> returnedItem = this.dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
            DiscordServer discordServer = new DiscordServer(guildId, returnedItem);

            if (!discordServer.hasServerBeenInitialized()) {
                try {
                    long startTime = System.currentTimeMillis();
                    initializeGambler(event, authorId, username, guildId, guildName, 0);
                    long endTime = System.currentTimeMillis();
                    long totalTime = endTime - startTime;
                    logger.info("Finished initialize player in " + totalTime + "ms for guild: " + guildName);

                } catch (Exception e) {
                    logger.info("Error initializing player for authorId {}, guildId {}", authorId, guildId);
                    logger.info(e.getMessage());
                    throw e;
                }
            }


            if (authorId.equalsIgnoreCase(event.getInteraction().getGuild().block().getOwnerId().asString())) {

                String usernameToRemove = event.getOption("username").get().getValue().get().asString();

                //remove user
                Map<String, AttributeValue> users = discordServer.getModifiableUsers();
                boolean userFound = false;
                for (String userId : users.keySet()) {
                    String currentUsername = users.get(userId).s().split("-")[Constants.USERNAME];
                    if (usernameToRemove.equalsIgnoreCase(currentUsername)) {
                        users.remove(userId);
                        userFound = true;
                        break;
                    }
                }
                if (userFound) {
                    HashMap<String, AttributeValue> updatedValues = new HashMap<>();
                    updatedValues.put(":" + Constants.USERS, AttributeValue.builder().m(users).build());
                    updatedValues.put(":" + Constants.USERS_VERSION, AttributeValue.builder().n(String.valueOf(discordServer.getUsersVersion() + 1)).build());
                    HashSet<String> keys = new HashSet<>();
                    keys.add(Constants.USERS);
                    keys.add(Constants.USERS_VERSION);
                    dynamoDBHelper.updateTableItem(guildId, updatedValues, keys);
                    return event.editReply(usernameToRemove + " has been removed.").then();

                } else {
                    return event.editReply("Username " + usernameToRemove + " not found.").then();
                }
            } else {
                return event.editReply("Only the server owner can remove users.").then();
            }
        } catch (ConditionalCheckFailedException e) {
            logger.info(guildId + ": " + e.getMessage());
            if (retries < 10) {
                return removeUser(event, authorId, guildId, guildName, username, retries + 1);
            } else {
                throw (e);
            }

        } catch (Exception e) {
            logger.info("Error removing user for authorId {}, guildId {}", authorId, guildId);
            logger.info(e.getMessage());
            return event.editReply().withContent("Issue removing user. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb").then();
        }
    }

//    private Mono<Void> configureReset(ChatInputInteractionEvent event, String authorId, String guildId, String guildName, String username) throws Exception {
//        Map<String, AttributeValue> returnedItem = this.dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
//        DiscordServer discordServer = new DiscordServer(guildId, returnedItem);
//
//        if (!discordServer.hasServerBeenInitialized()) {
//            try {
//                long startTime = System.currentTimeMillis();
//                initializeGambler(event, authorId, username, guildId, guildName, 0);
//                long endTime = System.currentTimeMillis();
//                long totalTime = endTime - startTime;
//                logger.info("Finished initialize player in " + totalTime + "ms for guild: " + guildName);
//
//            } catch (Exception e) {
//                logger.info("Error initializing player for authorId {}, guildId {}", authorId, guildId);
//                logger.info(e.getMessage());
//                throw e;
//            }
//        }
//
//
//        if (authorId.equalsIgnoreCase(event.getInteraction().getGuild().block().getOwnerId().asString())) {
//
//        } else {
//            return event.editReply("Only the server owner can configure resets.").then();
//        }
//
//    }

    private static void registerCommands(GatewayDiscordClient gateway) {
        List<String> commands = new ArrayList<>();
        commands.add("mypoints.json");
        commands.add("configure.json");
        commands.add("startprediction.json");
        commands.add("clearpredictions.json");
        commands.add("dailypoints.json");
        commands.add("leaderboard.json");
        commands.add("help.json");
        commands.add("coinflip.json");
        commands.add("roulette.json");
        commands.add("resetpoints.json");
        commands.add("creator.json");
        commands.add("awardpoints.json");
        commands.add("removepoints.json");
        commands.add("removeuser.json");
        //commands.add("configurereset.json");
        try {
            new GlobalCommandRegistrar(gateway.getRestClient()).registerCommands(commands);
        } catch (Exception e) {
            logger.info("Error registering commands.");
        }
    }

    private Mono<Message> myPoints(ChatInputInteractionEvent event, String guildId, String authorId, String username, String guildName) throws
            Exception {
        Map<String, AttributeValue> returnedItem;
        returnedItem = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
        DiscordServer discordServer = new DiscordServer(guildId, returnedItem);
        if (!discordServer.hasServerBeenInitialized() || !discordServer.userExists(authorId)) {
            try {
                long startTime = System.currentTimeMillis();
                initializeGambler(event, authorId, username, guildId, guildName, 0);
                long endTime = System.currentTimeMillis();
                long totalTime = endTime - startTime;
                returnedItem = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
                discordServer = new DiscordServer(guildId, returnedItem);
                logger.info("Finished initialize player in " + totalTime + "ms for guild: " + guildName);
            } catch (Exception e) {
                logger.info("Error initializing player for authorId {}, guildId {}", authorId, guildId);
                logger.info(e.getMessage());
                throw e;
            }
        }

        if (discordServer.userExists(authorId)) {
            String points = discordServer.getCurrentPoints(authorId);
            if (Integer.parseInt(points) == 0) {
                return event.editReply("You have " + points + " points use /dailypoints to gain more.");
            } else {
                return event.editReply("You have " + points + " points");
            }
        }
        return null;
    }

    private boolean initializeGambler(DeferrableInteractionEvent event, String authorId, String
            username, String guildId, String guildName, int retries) throws Exception {
        String currentPoints = "";
        username = username.replace("-", "");
        try {
            Map<String, AttributeValue> returnedItem = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
            DiscordServer discordServer = new DiscordServer(guildId, returnedItem);
            String startingPoints = "1000";
            if (discordServer.getStartingPoints() != null) {
                startingPoints = discordServer.getStartingPoints();
            }
            if (!discordServer.hasServerBeenInitialized()) {
                returnedItem = new HashMap<>();
                returnedItem.put(Constants.GUILD_ID, AttributeValue.builder().s(guildId).build());
                returnedItem.put(Constants.IN_PROGRESS_GAMBLE_DATA_ONE, AttributeValue.builder().s(Constants.NO_SESSION).build());
                Map<String, AttributeValue> usersMap = new HashMap<>();
                usersMap.put(authorId, AttributeValue.builder().s(startingPoints + "-" + username + "-" + dtf.format(LocalDateTime.now().minusDays(1))).build());
                returnedItem.put(Constants.USERS, AttributeValue.builder().m(usersMap).build());
                returnedItem.put(Constants.GUILD_NAME, AttributeValue.builder().s(guildName).build());
                returnedItem.put(Constants.USERS_VERSION, AttributeValue.builder().n("0").build());
                returnedItem.put(Constants.GAMBLERS_ONE_VERSION, AttributeValue.builder().n("0").build());
                returnedItem.put(Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_ONE, AttributeValue.builder().n("0").build());
                dynamoDBHelper.putItemInTable(returnedItem);
            } else if (discordServer.userExists(authorId)) {
                if (discordServer.hasUsernameChanged(authorId, username)) {

                    Map<String, AttributeValue> modifiableUserMap = discordServer.getModifiableUsers();
                    String[] data = modifiableUserMap.get(authorId).s().split("-");
                    modifiableUserMap.put(authorId, AttributeValue.builder().s(data[Constants.CURRENT_POINTS] + "-" + username + "-" + data[Constants.DAILY]).build());

                    HashMap<String, AttributeValue> updatedValues = new HashMap<>();
                    updatedValues.put(":" + Constants.USERS, AttributeValue.builder().m(modifiableUserMap).build());
                    updatedValues.put(":" + Constants.USERS_VERSION, AttributeValue.builder().n(String.valueOf(discordServer.getUsersVersion() + 1)).build());

                    HashSet<String> keys = new HashSet<>();
                    keys.add(Constants.USERS);
                    keys.add(Constants.USERS_VERSION);
                    dynamoDBHelper.updateTableItem(guildId, updatedValues, keys);
                }
            } else {
                Map<String, AttributeValue> modifiableUserMap = discordServer.getModifiableUsers();
                modifiableUserMap.put(authorId, AttributeValue.builder().s(startingPoints + "-" + username + "-" + dtf.format(LocalDateTime.now().minusDays(1))).build());
                HashMap<String, AttributeValue> updatedValues = new HashMap<>();
                updatedValues.put(":" + Constants.USERS, AttributeValue.builder().m(modifiableUserMap).build());
                updatedValues.put(":" + Constants.USERS_VERSION, AttributeValue.builder().n(String.valueOf(discordServer.getUsersVersion() + 1)).build());
                HashSet<String> keys = new HashSet<>();
                keys.add(Constants.USERS);
                keys.add(Constants.USERS_VERSION);

                dynamoDBHelper.updateTableItem(guildId, updatedValues, keys);
            }
        } catch (ConditionalCheckFailedException e) {
            logger.info(guildId + ": " + e.getMessage());
            if (retries < 10)
                return initializeGambler(event, authorId, username, guildId, guildName, retries + 1);
            throw (e);

        } catch (Exception e) {
            logger.info("Error initializing player for authorId {}, guildId {}", authorId, guildId);
            logger.info(e.getMessage());
        }
        return true;
    }

    private Mono<Message> startGamble(DeferrableInteractionEvent event, String guildId, String
            authorId, String username, String guildName, String title, String option1, String option2) {
        try {
            Map<String, AttributeValue> returnedItem = this.dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
            DiscordServer discordServer = new DiscordServer(guildId, returnedItem);

            if (!discordServer.hasServerBeenInitialized()) {
                try {
                    long startTime = System.currentTimeMillis();
                    initializeGambler(event, authorId, username, guildId, guildName, 0);
                    long endTime = System.currentTimeMillis();
                    long totalTime = endTime - startTime;
                    returnedItem = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
                    discordServer = new DiscordServer(guildId, returnedItem);
                    logger.info("Finished initialize player in " + totalTime + "ms for guild: " + guildName);
                } catch (Exception e) {
                    logger.info("Error initializing player for authorId {}, guildId {}", authorId, guildId);
                    logger.info(e.getMessage());
                    throw e;
                }
            }

            if (!StringUtils.isBlank(discordServer.getRole()) && !Constants.EVERYONE.equalsIgnoreCase(discordServer.getRole())) {
                boolean hasRole = false;
                for (Role role : event.getInteraction().getMember().get().getRoles().toIterable()) {
                    if (discordServer.getRole().equalsIgnoreCase(role.getName())) {
                        hasRole = true;
                    }
                }
                if (!hasRole) {
                    return event.editReply("Only users with role (" + discordServer.getRole() + ") can start a prediction session.");
                }
            }

            if (option1.equals(option2)) {
                return event.editReply("Options cannot be the same.");
            }
            if (discordServer.isGambleOneInProgress()) {

                if (discordServer.isGambleTwoInProgress()) {
                    if (discordServer.isGambleThreeInProgress()) {
                        return event.editReply("Three predictions already in session.");
                    } else {
                        //Initiate third gamble
                        String timer = String.valueOf(System.currentTimeMillis());
                        postEmbedAndStoreInDynamo(event, guildId, title, option1, option2, timer, Constants.IN_PROGRESS_GAMBLE_DATA_THREE, username);
                    }
                } else {
                    //Initiate second gamble
                    String timer = String.valueOf(System.currentTimeMillis());
                    postEmbedAndStoreInDynamo(event, guildId, title, option1, option2, timer, Constants.IN_PROGRESS_GAMBLE_DATA_TWO, username);
                }
            } else {
                String timer = String.valueOf(System.currentTimeMillis());

                postEmbedAndStoreInDynamo(event, guildId, title, option1, option2, timer, Constants.IN_PROGRESS_GAMBLE_DATA_ONE, username);

                return event.editReply("New prediction session in progress!");
            }
        } catch (Exception e) {
            if (e.getMessage() != null && (e.getMessage().contains("50013") || e.getMessage().contains("50001"))){
                return event.editReply("I need permission to send messages in this channel.");
            }
            logger.info("Error starting prediction for authorId {}, guildId {}", authorId, guildId);
            logger.info(e.getMessage());
            return event.editReply().withContent("Issue starting prediction, please verify inputs follow format /startprediction (title) (option1) (option2). Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb");
        }
        return event.editReply("Prediction started.");

    }

    private void postEmbedAndStoreInDynamo(DeferrableInteractionEvent event, String guildId, String title, String option1, String option2, String timer, String inProgressGambleDataKey, String username) {
        EmbedData embed = EmbedData.builder()
                .color(1)
                .title(title)
                .addField(EmbedFieldData.builder().name("Options:").value("Pots:").inline(true).build())
                .addField(EmbedFieldData.builder().name(option1).value("0").inline(true).build())
                .addField(EmbedFieldData.builder().name(option2).value("0").inline(true).build())
                .addField(EmbedFieldData.builder().name("").value("Use the buttons below to join, lock or cancel the prediction.").inline(false).build())
                .timestamp(String.valueOf(Instant.now()))
                .footer(EmbedFooterData.builder().text("Created by " + username).build())
                .build();
        Button button = Button.primary(Constants.OPTION_1_BUTTON, option1);
        Button button2 = Button.primary(Constants.OPTION_2_BUTTON, option2);
        Button button3 = Button.success(Constants.LOCK_BUTTON, "Lock");
        List<Button> buttons = new ArrayList<>();
        buttons.add(button);
        buttons.add(button2);
        buttons.add(button3);
        MessageCreateRequest messageCreateRequest = MessageCreateRequest.builder().addEmbed(embed).addComponent(ActionRow.of(buttons).getData()).build();
        MessageData messageData = client.getChannelById(event.getInteraction().getChannelId()).createMessage(messageCreateRequest).block();
        String messageId = messageData.id().asString();

        HashMap<String, AttributeValue> updatedValues = new HashMap<>();
        updatedValues.put(":" + inProgressGambleDataKey, AttributeValue.builder().s(title + "-" + option1 + "-" + option2 + "-" + timer + "-0-0-" + messageId).build());
        HashSet<String> keys = new HashSet<>();
        keys.add(inProgressGambleDataKey);
        dynamoDBHelper.updateTableItem(guildId, updatedValues, keys);
    }

    private Mono<Message> join(ModalSubmitInteractionEvent event, String authorId, String username, String
            guildId, String optionNumber, String guildName, int retries) throws Exception {
        try {
            Map<String, AttributeValue> returnedItem = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
            DiscordServer discordServer = new DiscordServer(guildId, returnedItem);
            String messageId = event.getInteraction().getData().message().get().id().asString();
            String inProgressGamble;
            GambleData gambleData;
            Map<String, AttributeValue> gamblers;
            int gamblersVersion;
            int inProgressGambleVersion;
            String inProgressGambleKey;
            String gamblersKey;
            String gamblersVersionKey;
            String inProgressGambleVersionKey;
            if (messageId.equalsIgnoreCase(discordServer.getPredictionOne().getMessageId())) {
                gambleData = discordServer.getPredictionOne();
                inProgressGambleVersion = discordServer.getInProgressGambleOneVersion();
                gamblers = discordServer.getModifiableGamblers(discordServer.getGamblersOne());
                gamblersVersion = discordServer.getGamblersOneVersion();
                inProgressGambleKey = Constants.IN_PROGRESS_GAMBLE_DATA_ONE;
                gamblersKey = Constants.GAMBLERS_ONE;
                gamblersVersionKey = Constants.GAMBLERS_ONE_VERSION;
                inProgressGambleVersionKey = Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_ONE;
            } else if (messageId.equalsIgnoreCase(discordServer.getPredictionTwo().getMessageId())) {
                gambleData = discordServer.getPredictionTwo();
                inProgressGambleVersion = discordServer.getInProgressGambleTwoVersion();
                gamblers = discordServer.getModifiableGamblers(discordServer.getGamblersTwo());
                gamblersVersion = discordServer.getGamblersTwoVersion();
                inProgressGambleKey = Constants.IN_PROGRESS_GAMBLE_DATA_TWO;
                gamblersKey = Constants.GAMBLERS_TWO;
                gamblersVersionKey = Constants.GAMBLERS_TWO_VERSION;
                inProgressGambleVersionKey = Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_TWO;
            } else if (messageId.equalsIgnoreCase(discordServer.getPredictionThree().getMessageId())) {
                gambleData = discordServer.getPredictionThree();
                inProgressGambleVersion = discordServer.getInProgressGambleThreeVersion();
                gamblers = discordServer.getModifiableGamblers(discordServer.getGamblersThree());
                gamblersVersion = discordServer.getGamblersThreeVersion();
                inProgressGambleKey = Constants.IN_PROGRESS_GAMBLE_DATA_THREE;
                gamblersKey = Constants.GAMBLERS_THREE;
                gamblersVersionKey = Constants.GAMBLERS_THREE_VERSION;
                inProgressGambleVersionKey = Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_THREE;
            } else {
                return event.editReply("Prediction no longer exists.");
            }
            String option;
            if (optionNumber.equalsIgnoreCase(Constants.OPTION_1_BUTTON)) {
                option = event.getInteraction().getData().message().get().embeds().get(0).fields().get().get(1).name();
            } else {
                option = event.getInteraction().getData().message().get().embeds().get(0).fields().get().get(2).name();
            }
            String pointsWagered = event.getComponents(TextInput.class).get(0).getValue().get();

            if (gamblers.containsKey(authorId)) {
                return event.editReply("You have already placed a bet.");
            }

            if (!discordServer.userExists(authorId)) {
                try {
                    long startTime = System.currentTimeMillis();
                    initializeGambler(event, authorId, username, guildId, guildName, 0);
                    long endTime = System.currentTimeMillis();
                    long totalTime = endTime - startTime;
                    returnedItem = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
                    discordServer = new DiscordServer(guildId, returnedItem);
                    logger.info("Finished initialize player in " + totalTime + "ms for guild: " + guildName);
                } catch (Exception e) {
                    logger.info("Error initializing player for authorId {}, guildId {}", authorId, guildId);
                    logger.info(e.getMessage());
                    throw e;
                }
            }
            int pointsBetI = 0;
            int currentPointsI = Integer.parseInt(discordServer.getCurrentPoints(authorId));
            try {
                pointsBetI = Integer.parseInt(pointsWagered);
                if (pointsBetI <= 0) {
                    throw new NumberFormatException();
                }
            } catch (NumberFormatException e) {
                return event.editReply("Invalid wager amount.");
            }
            if (currentPointsI < pointsBetI) {
                return event.editReply("Not enough points to join.");
            }

            gamblers.put(authorId, AttributeValue.builder().s(option + "-" + pointsWagered).build());
            Map<String, AttributeValue> users = discordServer.getModifiableUsers();
            String user = users.get(authorId).s();
            String[] userArray = user.split("-");
            String daily = userArray[Constants.DAILY];
            String newPoints = String.valueOf(currentPointsI - pointsBetI);
            String newUserData = newPoints + "-" + username + "-" + daily;
            users.put(authorId, AttributeValue.builder().s(newUserData).build());
            String newInProgressGambleData;
            int option1Pot;
            int option2Pot;
            if (option.equalsIgnoreCase(gambleData.getOption1())) {
                option1Pot = Integer.parseInt(gambleData.getOption1Pot()) + pointsBetI;
                option2Pot = Integer.parseInt(gambleData.getOption2Pot());
            } else {
                option1Pot = Integer.parseInt(gambleData.getOption1Pot());
                option2Pot = Integer.parseInt(gambleData.getOption2Pot()) + pointsBetI;
            }
            newInProgressGambleData = gambleData.getTitle() + "-"
                    + gambleData.getOption1() + "-"
                    + gambleData.getOption2() + "-"
                    + gambleData.getTimer() + "-"
                    + option1Pot + "-"
                    + option2Pot + "-"
                    + gambleData.getMessageId();
            addGamblerToASession(guildId, discordServer, gamblers, newInProgressGambleData, users, discordServer.getUsersVersion(), gamblersVersion, inProgressGambleVersion
                    , inProgressGambleKey, gamblersKey, gamblersVersionKey, inProgressGambleVersionKey);

            EmbedData embed = EmbedData.builder()
                    .color(1)
                    .title(gambleData.getTitle())
                    .addField(EmbedFieldData.builder().name("Options:").value("Pots:").inline(true).build())
                    .addField(EmbedFieldData.builder().name(gambleData.getOption1()).value(String.valueOf(option1Pot)).inline(true).build())
                    .addField(EmbedFieldData.builder().name(gambleData.getOption2()).value(String.valueOf(option2Pot)).inline(true).build())
                    .addField(EmbedFieldData.builder().name("").value("Use the buttons below to join, lock or cancel the prediction.").inline(false).build())
                    .timestamp(String.valueOf(Instant.now()))
                    .footer(EmbedFooterData.builder().text("Updated").build())
                    .build();
            RestMessage message = client.getMessageById(event.getInteraction().getChannelId(), Snowflake.of(messageId));
            message.edit(MessageEditRequest.builder().addEmbed(embed).build()).subscribe();
            return event.editReply("You have wagered " + pointsWagered + " on " + option + ".");

        } catch (ConditionalCheckFailedException e) {
            logger.info(guildId + ": " + e.getMessage());
            if (retries < 10) {
                return join(event, authorId, username, guildId, optionNumber, guildName, retries + 1);
            } else {
                throw (e);
            }

        } catch (Exception e) {
            logger.info("Error joining prediction for authorId {}, guildId {}", authorId, guildId);
            logger.info(e.getMessage());
            return event.editReply().withContent("Issue joining prediction. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb");
        }
    }

    private void addGamblerToASession(String guildId, DiscordServer
            discordServer, Map<String, AttributeValue> modifiableGamblerMap, String newInProgressGambleData, Map<String, AttributeValue> users, int usersVersion, int gamblersVersion, int inProgressGambleVersion,
                                      String inProgressGambleKey, String gamblersKey, String gamblersVersionKey, String inProgressGambleVersionKey) {
        HashMap<String, AttributeValue> mapToPut = new HashMap<>();
        mapToPut.put(":" + inProgressGambleKey, AttributeValue.builder().s(newInProgressGambleData).build());
        mapToPut.put(":" + gamblersKey, AttributeValue.builder().m(modifiableGamblerMap).build());
        mapToPut.put(":" + Constants.USERS, AttributeValue.builder().m(users).build());
        mapToPut.put(":" + gamblersVersionKey, AttributeValue.builder().n(String.valueOf(gamblersVersion + 1)).build());
        mapToPut.put(":" + inProgressGambleVersionKey, AttributeValue.builder().n(String.valueOf(inProgressGambleVersion + 1)).build());
        mapToPut.put(":" + Constants.USERS_VERSION, AttributeValue.builder().n(String.valueOf(usersVersion + 1)).build());
        HashSet<String> keys = new HashSet<>();
        keys.add(inProgressGambleKey);
        keys.add(inProgressGambleVersionKey);
        keys.add(Constants.USERS);
        keys.add(Constants.USERS_VERSION);
        keys.add(gamblersKey);
        keys.add(gamblersVersionKey);
        dynamoDBHelper.updateTableItem(guildId, mapToPut, keys);
    }

    private Mono<Void> endGamble(SelectMenuInteractionEvent event, String guildId, String authorId, String username, String
            guildName, int retries) throws Exception {
        try {
            Map<String, AttributeValue> returnedItem = this.dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
            DiscordServer discordServer = new DiscordServer(guildId, returnedItem);

            if (!StringUtils.isBlank(discordServer.getRole()) && !Constants.EVERYONE.equalsIgnoreCase(discordServer.getRole())) {
                boolean hasRole = false;
                for (Role role : event.getInteraction().getMember().get().getRoles().toIterable()) {
                    if (discordServer.getRole().equalsIgnoreCase(role.getName())) {
                        hasRole = true;
                    }
                }
                if (!hasRole) {
                    return event.editReply("Only users with role (" + discordServer.getRole() + ") can end a prediction session.").then();
                }
            }


            String winningOption = event.getValues().get(0);
            String messageId = event.getInteraction().getData().message().get().id().asString();

            String gamblersKey;
            String inProgressGambleKey;
            String gamblersVersionKey;
            String inProgressGambleDataVersionKey;
            Map<String, AttributeValue> gamblers;
            Map<String, AttributeValue> users;
            String title;
            String option1;
            String option2;
            String option1Pot;
            String option2Pot;
            String inProgressGambleData;
            int gamblersVersion;
            int inProgressGambleDataVersion;

            if (messageId.equalsIgnoreCase(discordServer.getPredictionOne().getMessageId())) {
                gamblersKey = Constants.GAMBLERS_ONE;
                inProgressGambleKey = Constants.IN_PROGRESS_GAMBLE_DATA_ONE;
                gamblersVersionKey = Constants.GAMBLERS_ONE_VERSION;
                inProgressGambleDataVersionKey = Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_ONE;
                inProgressGambleData = discordServer.getPredictionOne().toString();
                gamblers = discordServer.getGamblersOne();
                title = discordServer.getPredictionOne().getTitle();
                option1 = discordServer.getPredictionOne().getOption1();
                option2 = discordServer.getPredictionOne().getOption2();
                option1Pot = discordServer.getPredictionOne().getOption1Pot();
                option2Pot = discordServer.getPredictionOne().getOption2Pot();
                gamblersVersion = discordServer.getGamblersOneVersion();
                inProgressGambleDataVersion = discordServer.getInProgressGambleOneVersion();


            } else if (messageId.equalsIgnoreCase(discordServer.getPredictionTwo().getMessageId())) {
                gamblersKey = Constants.GAMBLERS_TWO;
                inProgressGambleKey = Constants.IN_PROGRESS_GAMBLE_DATA_TWO;
                gamblersVersionKey = Constants.GAMBLERS_TWO_VERSION;
                inProgressGambleDataVersionKey = Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_TWO;
                inProgressGambleData = discordServer.getPredictionTwo().toString();
                gamblers = discordServer.getGamblersTwo();
                title = discordServer.getPredictionTwo().getTitle();
                option1 = discordServer.getPredictionTwo().getOption1();
                option2 = discordServer.getPredictionTwo().getOption2();
                option1Pot = discordServer.getPredictionTwo().getOption1Pot();
                option2Pot = discordServer.getPredictionTwo().getOption2Pot();
                gamblersVersion = discordServer.getGamblersTwoVersion();
                inProgressGambleDataVersion = discordServer.getInProgressGambleTwoVersion();
            } else if (messageId.equalsIgnoreCase(discordServer.getPredictionThree().getMessageId())) {
                gamblersKey = Constants.GAMBLERS_THREE;
                inProgressGambleKey = Constants.IN_PROGRESS_GAMBLE_DATA_THREE;
                gamblersVersionKey = Constants.GAMBLERS_THREE_VERSION;
                inProgressGambleDataVersionKey = Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_THREE;
                inProgressGambleData = discordServer.getPredictionThree().toString();
                gamblers = discordServer.getGamblersThree();
                title = discordServer.getPredictionThree().getTitle();
                option1 = discordServer.getPredictionThree().getOption1();
                option2 = discordServer.getPredictionThree().getOption2();
                option1Pot = discordServer.getPredictionThree().getOption1Pot();
                option2Pot = discordServer.getPredictionThree().getOption2Pot();
                gamblersVersion = discordServer.getGamblersThreeVersion();
                inProgressGambleDataVersion = discordServer.getInProgressGambleThreeVersion();
            } else {
                return event.editReply("Prediction not found. You can only modify the last three predictions.").then();
            }
            if ("Cancel".equalsIgnoreCase(winningOption)) {
                users = discordServer.getModifiableUsers();
                refundPrediction(discordServer, gamblers, users);
                endGambleSession(guildId, users, discordServer, true, null,
                        gamblersKey, inProgressGambleKey, gamblersVersionKey, inProgressGambleDataVersionKey,
                        null, null, inProgressGambleData, gamblersVersion, inProgressGambleDataVersion);
                EmbedData embed = EmbedData.builder()
                        .color(1)
                        .title("~~" + title + "~~")
                        .addField(EmbedFieldData.builder().name("Options:").value("Pots:").inline(true).build())
                        .addField(EmbedFieldData.builder().name(option1).value(option1Pot).inline(true).build())
                        .addField(EmbedFieldData.builder().name(option2).value(option2Pot).inline(true).build())
                        .addField(EmbedFieldData.builder().name("").value("This prediction has been **canceled**. Start a new prediction with /startprediction.").inline(false).build())
                        .timestamp(String.valueOf(Instant.now()))
                        .footer(EmbedFooterData.builder().text("Canceled by " + username).build())
                        .build();
                RestMessage message = client.getMessageById(event.getInteraction().getChannelId(), Snowflake.of(messageId));
                Button button3 = Button.success(Constants.LOCK_BUTTON, "Unlock");
                button3 = button3.disabled();
                List<Button> buttons = new ArrayList<>();
                buttons.add(button3);
                Button replay_button = Button.success(Constants.REPLAY_BUTTON, "Replay");
                buttons.add(replay_button);

                SelectMenu selectMenu = SelectMenu.of("OUTCOME",
                        SelectMenu.Option.of(option1, option1),
                        SelectMenu.Option.of(option2, option2),
                        SelectMenu.Option.of("Cancel Prediction", Constants.CANCEL)
                ).withPlaceholder("Select Outcome To End Prediction");
                selectMenu = selectMenu.disabled();


                message.edit(MessageEditRequest.builder().addEmbed(embed).addComponent(ActionRow.of(selectMenu).getData()).addComponent(ActionRow.of(buttons).getData()).build()).subscribe();

                return event.deleteReply();
            }

            if (gamblers == null || Integer.parseInt(option1Pot) <= 0 || Integer.parseInt(option2Pot) <= 0) {
                SelectMenu selectMenu = SelectMenu.of("OUTCOME",
                        SelectMenu.Option.of(option1, option1),
                        SelectMenu.Option.of(option2, option2),
                        SelectMenu.Option.of("Cancel Prediction", Constants.CANCEL)
                ).withPlaceholder("Select Outcome To End Prediction");
                Button button3 = Button.success(Constants.LOCK_BUTTON, "Unlock");
                List<Button> buttons = new ArrayList<>();
                buttons.add(button3);

                RestMessage message = client.getMessageById(event.getInteraction().getChannelId(), Snowflake.of(messageId));

                message.edit(MessageEditRequest.builder().addComponent(ActionRow.of(selectMenu).getData()).addComponent(ActionRow.of(buttons).getData()).build()).subscribe();

                return event.editReply().withContent("Wait for more joiners. To cancel the prediction, click lock then select cancel prediction.").then();
            }

            Map<String, AttributeValue> modifiableUserMap = discordServer.getModifiableUsers();
            Map<String, AttributeValue> winLoseUserMap = new HashMap<>();
            String results = "Outcome: " + winningOption + "\n ---------------------------------------------------\n";
            for (Map.Entry<String, AttributeValue> gambler : gamblers.entrySet()) {
                int currentPoints = Integer.parseInt(discordServer.getCurrentPoints(gambler.getKey()));
                int pointsBet = Integer.parseInt(gambler.getValue().s().split("-")[Constants.POINTS_BET]);
                Double pointsBetD = Double.parseDouble(gambler.getValue().s().split("-")[Constants.POINTS_BET]);
                String option = gambler.getValue().s().split("-")[Constants.OPTION];
                String gamblerUsername = discordServer.getUsername(gambler.getKey());
                String[] userData = modifiableUserMap.get(gambler.getKey()).s().split("-");
                String newUserPoints;
                long amountWon;
                if (winningOption.equalsIgnoreCase(option)) {
                    if (winningOption.equalsIgnoreCase(option1)) {
                        amountWon = (long) ((pointsBetD / Double.parseDouble(option1Pot)) * Double.parseDouble(option2Pot));
                    } else {
                        amountWon = (long) ((pointsBetD / Double.parseDouble(option2Pot)) * Double.parseDouble(option1Pot));
                    }
                    if (amountWon + currentPoints + pointsBet > Integer.MAX_VALUE) {
                        results += gamblerUsername + " WINS and is at max points!\n";
                        newUserPoints = String.valueOf(Integer.MAX_VALUE);
                        modifiableUserMap.put(gambler.getKey(), AttributeValue.builder().s(newUserPoints + "-" + gamblerUsername + "-" + userData[Constants.DAILY]).build());

                    } else {
                        results += gamblerUsername + " WINS: " + amountWon + "\n";
                        newUserPoints = String.valueOf(currentPoints + amountWon + pointsBet);
                        winLoseUserMap.put(gambler.getKey(), AttributeValue.builder().s("WIN-" + amountWon).build());
                        modifiableUserMap.put(gambler.getKey(), AttributeValue.builder().s(newUserPoints + "-" + gamblerUsername + "-" + userData[Constants.DAILY]).build());
                    }
                } else {
                    results += gamblerUsername + " LOSES: " + pointsBet + "\n";
                    winLoseUserMap.put(gambler.getKey(), AttributeValue.builder().s("LOSS-" + pointsBet).build());
                }
            }
            String previousGamblersKey;
            String previousGamblerDataKey;
            String timer;
            if (discordServer.getPreviousPredictionOne() == null || StringUtil.isNullOrEmpty(discordServer.getPreviousPredictionOne().getTitle())) {
                previousGamblersKey = Constants.PREVIOUS_GAMBLERS_ONE;
                previousGamblerDataKey = Constants.PREVIOUS_PREDICTION_ONE;
            } else if (discordServer.getPreviousPredictionTwo() == null || StringUtil.isNullOrEmpty(discordServer.getPreviousPredictionTwo().getTitle())) {
                previousGamblersKey = Constants.PREVIOUS_GAMBLERS_TWO;
                previousGamblerDataKey = Constants.PREVIOUS_PREDICTION_TWO;
            } else if (discordServer.getPreviousPredictionThree() == null || StringUtil.isNullOrEmpty(discordServer.getPreviousPredictionThree().getTitle())) {
                previousGamblersKey = Constants.PREVIOUS_GAMBLERS_THREE;
                previousGamblerDataKey = Constants.PREVIOUS_PREDICTION_THREE;
            } else {
                previousGamblersKey = Constants.PREVIOUS_GAMBLERS_ONE;
                previousGamblerDataKey = Constants.PREVIOUS_PREDICTION_ONE;
                timer = discordServer.getPreviousPredictionOne().getTimer();

                if (Long.parseLong(discordServer.getPreviousPredictionTwo().getTimer()) < Long.parseLong(timer)) {
                    previousGamblersKey = Constants.PREVIOUS_GAMBLERS_TWO;
                    previousGamblerDataKey = Constants.PREVIOUS_PREDICTION_TWO;
                    timer = discordServer.getPreviousPredictionTwo().getTimer();
                }

                if (Long.parseLong(discordServer.getPreviousPredictionThree().getTimer()) < Long.parseLong(timer)) {
                    previousGamblersKey = Constants.PREVIOUS_GAMBLERS_THREE;
                    previousGamblerDataKey = Constants.PREVIOUS_PREDICTION_THREE;
                    timer = discordServer.getPreviousPredictionThree().getTimer();
                }

            }
            endGambleSession(guildId, modifiableUserMap, discordServer, false, winLoseUserMap, gamblersKey,
                    inProgressGambleKey, gamblersVersionKey, inProgressGambleDataVersionKey, previousGamblersKey,
                    previousGamblerDataKey, inProgressGambleData, gamblersVersion, inProgressGambleDataVersion);
            EmbedData embed = EmbedData.builder()
                    .color(1)
                    .title(title)
                    .addField(EmbedFieldData.builder().name("Options:").value("Pots:").inline(true).build())
                    .addField(EmbedFieldData.builder().name(option1).value(option1Pot).inline(true).build())
                    .addField(EmbedFieldData.builder().name(option2).value(option2Pot).inline(true).build())
                    .addField(EmbedFieldData.builder().name("").value(results).inline(false).build())
                    .timestamp(String.valueOf(Instant.now()))
                    .footer(EmbedFooterData.builder().text("Ended by " + username).build())
                    .build();
            Button button3 = Button.success(Constants.LOCK_BUTTON, "Unlock");
            button3 = button3.disabled();
            List<Button> buttons = new ArrayList<>();
            buttons.add(button3);
            Button replay_button = Button.success(Constants.REPLAY_BUTTON, "Replay");
            buttons.add(replay_button);
            Button refund_button = Button.danger(Constants.REFUND_BUTTON, "Refund");
            buttons.add(refund_button);

            SelectMenu selectMenu = SelectMenu.of("OUTCOME",
                    SelectMenu.Option.of(option1, option1),
                    SelectMenu.Option.of(option2, option2),
                    SelectMenu.Option.of("Cancel Prediction", Constants.CANCEL)
            ).withPlaceholder("Select Outcome To End Prediction");
            selectMenu = selectMenu.disabled();
            RestMessage message = client.getMessageById(event.getInteraction().getChannelId(), Snowflake.of(messageId));
            message.edit(MessageEditRequest.builder().addComponent(ActionRow.of(selectMenu).getData()).addEmbed(embed).addComponent(ActionRow.of(buttons).getData()).build()).subscribe();
        } catch (ConditionalCheckFailedException e) {
            if (retries < 10) {
                endGamble(event, guildId, authorId, username, guildName, retries + 1);
            } else {
                throw (e);
            }
        } catch (Exception e) {
            logger.info("Error ending prediction for authorId {}, guildId {}", authorId, guildId);
            logger.info(e.getMessage());
            return event.editReply().withContent("Issue ending prediction. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb").then();
        }
        return event.deleteReply();
    }

    private void endGambleSession(String guildId, Map<String, AttributeValue> modifiableUserMap, DiscordServer discordServer, boolean wasCanceled, Map<String, AttributeValue> winLoseUserMap
            , String gamblersKey, String inProgressGambleDataKey, String gamblersVersionKey, String inProgressGambleDataVersionKey
            , String previousGamblersKey, String previousGambleDataKey, String inProgressGambleData, int gamblersVersion, int inProgressGambleDataVersion) {
        HashMap<String, AttributeValue> mapToPut = new HashMap<>();
        mapToPut.put(":" + gamblersKey, AttributeValue.builder().m(new HashMap<>()).build());
        mapToPut.put(":" + inProgressGambleDataKey, AttributeValue.builder().s(Constants.NO_SESSION).build());
        mapToPut.put(":" + Constants.USERS, AttributeValue.builder().m(modifiableUserMap).build());
        mapToPut.put(":" + gamblersVersionKey, AttributeValue.builder().n(String.valueOf(gamblersVersion + 1)).build());
        mapToPut.put(":" + inProgressGambleDataVersionKey, AttributeValue.builder().n(String.valueOf(inProgressGambleDataVersion + 1)).build());
        mapToPut.put(":" + Constants.USERS_VERSION, AttributeValue.builder().n(String.valueOf(discordServer.getUsersVersion() + 1)).build());
        HashSet<String> keys = new HashSet<>();
        if (!wasCanceled) {
            mapToPut.put(":" + previousGamblersKey, AttributeValue.builder().m(winLoseUserMap).build());
            keys.add(previousGamblersKey);

            mapToPut.put(":" + previousGambleDataKey, AttributeValue.builder().s(inProgressGambleData).build());
            keys.add(previousGambleDataKey);
        }

        keys.add(inProgressGambleDataKey);
        keys.add(gamblersKey);
        keys.add(Constants.USERS);
        keys.add(Constants.USERS_VERSION);
        keys.add(gamblersVersionKey);
        keys.add(inProgressGambleDataVersionKey);
        dynamoDBHelper.updateTableItem(guildId, mapToPut, keys);
    }

    private void refundGambleSession(String guildId, Map<String, AttributeValue> modifiableUserMap, DiscordServer discordServer, String previousGamblerKey, String previousPredictionKey) {
        HashMap<String, AttributeValue> mapToPut = new HashMap<>();
        mapToPut.put(":" + Constants.USERS, AttributeValue.builder().m(modifiableUserMap).build());
        mapToPut.put(":" + Constants.USERS_VERSION, AttributeValue.builder().n(String.valueOf(discordServer.getUsersVersion() + 1)).build());
        mapToPut.put(":" + previousGamblerKey, AttributeValue.builder().m(new HashMap<>()).build());
        mapToPut.put(":" + previousPredictionKey, AttributeValue.builder().s("").build());
        HashSet<String> keys = new HashSet<>();
        keys.add(previousGamblerKey);
        keys.add(previousPredictionKey);
        keys.add(Constants.USERS);
        keys.add(Constants.USERS_VERSION);
        dynamoDBHelper.updateTableItem(guildId, mapToPut, keys);
    }

    private static void resetPoints(String guildId, Map<String, AttributeValue> modifiableUserMap, DiscordServer discordServer, String startingPoints) {
        HashMap<String, AttributeValue> mapToPut = new HashMap<>();
        mapToPut.put(":" + Constants.USERS, AttributeValue.builder().m(getResetUserMap(modifiableUserMap, startingPoints)).build());
        mapToPut.put(":" + Constants.USERS_RESET, AttributeValue.builder().m(modifiableUserMap).build());
        mapToPut.put(":" + Constants.PREVIOUS_GAMBLERS_ONE, AttributeValue.builder().m(new HashMap<>()).build());
        mapToPut.put(":" + Constants.PREVIOUS_GAMBLERS_TWO, AttributeValue.builder().m(new HashMap<>()).build());
        mapToPut.put(":" + Constants.PREVIOUS_GAMBLERS_THREE, AttributeValue.builder().m(new HashMap<>()).build());
        mapToPut.put(":" + Constants.GAMBLERS_ONE, AttributeValue.builder().m(new HashMap<>()).build());
        mapToPut.put(":" + Constants.GAMBLERS_TWO, AttributeValue.builder().m(new HashMap<>()).build());
        mapToPut.put(":" + Constants.GAMBLERS_THREE, AttributeValue.builder().m(new HashMap<>()).build());
        mapToPut.put(":" + Constants.IN_PROGRESS_GAMBLE_DATA_ONE, AttributeValue.builder().s(Constants.NO_SESSION).build());
        mapToPut.put(":" + Constants.IN_PROGRESS_GAMBLE_DATA_TWO, AttributeValue.builder().s(Constants.NO_SESSION).build());
        mapToPut.put(":" + Constants.IN_PROGRESS_GAMBLE_DATA_THREE, AttributeValue.builder().s(Constants.NO_SESSION).build());
        mapToPut.put(":" + Constants.GAMBLERS_ONE_VERSION, AttributeValue.builder().n("0").build());
        mapToPut.put(":" + Constants.GAMBLERS_TWO_VERSION, AttributeValue.builder().n("0").build());
        mapToPut.put(":" + Constants.GAMBLERS_THREE_VERSION, AttributeValue.builder().n("0").build());
        mapToPut.put(":" + Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_ONE, AttributeValue.builder().n("0").build());
        mapToPut.put(":" + Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_TWO, AttributeValue.builder().n("0").build());
        mapToPut.put(":" + Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_THREE, AttributeValue.builder().n("0").build());
        mapToPut.put(":" + Constants.USERS_VERSION, AttributeValue.builder().n("0").build());
        HashSet<String> keys = new HashSet<>();
        keys.add(Constants.IN_PROGRESS_GAMBLE_DATA_ONE);
        keys.add(Constants.IN_PROGRESS_GAMBLE_DATA_TWO);
        keys.add(Constants.IN_PROGRESS_GAMBLE_DATA_THREE);
        keys.add(Constants.GAMBLERS_ONE);
        keys.add(Constants.GAMBLERS_TWO);
        keys.add(Constants.GAMBLERS_THREE);
        keys.add(Constants.USERS_RESET);
        keys.add(Constants.USERS_VERSION);
        keys.add(Constants.GAMBLERS_ONE_VERSION);
        keys.add(Constants.GAMBLERS_TWO_VERSION);
        keys.add(Constants.GAMBLERS_THREE_VERSION);
        keys.add(Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_ONE);
        keys.add(Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_TWO);
        keys.add(Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_THREE);
        keys.add(Constants.PREVIOUS_GAMBLERS_ONE);
        keys.add(Constants.PREVIOUS_GAMBLERS_TWO);
        keys.add(Constants.PREVIOUS_GAMBLERS_THREE);
        keys.add(Constants.USERS);
        dynamoDBHelper.updateTableItem(guildId, mapToPut, keys);
    }

    public static Map<String, AttributeValue> getResetUserMap(Map<String, AttributeValue> modifiableUserMap, String startingPoints) {
        Map<String, AttributeValue> resetUserMap = new HashMap<>();
        for (String userId : modifiableUserMap.keySet()) {
            String userData = modifiableUserMap.get(userId).s();
            User user = new User(userData);
            user.setPoints(startingPoints);
            resetUserMap.put(userId, AttributeValue.builder().s(user.getUserAsString()).build());
        }

        return resetUserMap;
    }

    public Mono<Void> dailyPoints(String guildName, String guildId, String authorId, ChatInputInteractionEvent event, String username,
                                  int retries) throws Exception {
        Map<String, AttributeValue> returnedItem = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
        DiscordServer discordServer = new DiscordServer(guildId, returnedItem);
        if (!discordServer.userExists(authorId)) {
            try {
                long startTime = System.currentTimeMillis();
                initializeGambler(event, authorId, username, guildId, guildName, 0);
                long endTime = System.currentTimeMillis();
                long totalTime = endTime - startTime;
                returnedItem = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
                discordServer = new DiscordServer(guildId, returnedItem);
                logger.info("Finished initialize player in " + totalTime + "ms for guild: " + guildName);
            } catch (Exception e) {
                logger.info("Error initializing player for authorId {}, guildId {}", authorId, guildId);
                logger.info(e.getMessage());
                throw e;
            }
        }
        LocalDate daily = LocalDate.parse(discordServer.getDaily(authorId), dtf);
        ZonedDateTime nowCST = ZonedDateTime.now(ZoneId.of("America/Chicago"));
        logger.debug("Daily points requested at current date: " + nowCST.toString());
        if (dtf.format(nowCST).equals(dtf.format(daily))) {
            return event.editReply("Already used dailypoints today.").then();
        } else {
            String newPoints;
            Map<String, AttributeValue> newUserMapUpdate = discordServer.getModifiableUsers();
            int dailyPointAmount = 100;
            if (discordServer.getDailyPoints() != null) {
                dailyPointAmount = Integer.parseInt(discordServer.getDailyPoints());
            }
            if (Long.parseLong(discordServer.getCurrentPoints(authorId)) + dailyPointAmount > Integer.MAX_VALUE) {
                newPoints = String.valueOf(Integer.MAX_VALUE);
            } else {
                newPoints = String.valueOf(Integer.parseInt(discordServer.getCurrentPoints(authorId)) + dailyPointAmount);
            }
            newUserMapUpdate.put(authorId, AttributeValue.builder().s(newPoints + "-" + username + "-" + dtf.format(nowCST)).build());
            HashMap<String, AttributeValue> updatedValues = new HashMap<>();
            updatedValues.put(":" + Constants.USERS, AttributeValue.builder().m(newUserMapUpdate).build());
            updatedValues.put(":" + Constants.USERS_VERSION, AttributeValue.builder().n(String.valueOf(discordServer.getUsersVersion() + 1)).build());
            HashSet<String> keys = new HashSet<>();
            keys.add(Constants.USERS);
            keys.add(Constants.USERS_VERSION);
            try {
                dynamoDBHelper.updateTableItem(guildId, updatedValues, keys);
            } catch (ConditionalCheckFailedException e) {
                if (retries < 10) {
                    dailyPoints(guildName, guildId, authorId, event, username, retries + 1);
                } else {
                    throw (e);
                }
            }

            if (Long.parseLong(newPoints) >= Integer.MAX_VALUE) {
                return event.editReply("You are at max points! You have " + newPoints + " points.").then();
            }
            return event.editReply("You now have " + newPoints + " points.").then();
        }
    }

    public Mono<Void> leaderboard(String guildId, ChatInputInteractionEvent event, String authorId, String username, String guildName, boolean isReset) {
        try {
            Map<String, AttributeValue> returnedItem = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
            DiscordServer discordServer = new DiscordServer(guildId, returnedItem);
            if (!discordServer.hasServerBeenInitialized()) {
                try {
                    long startTime = System.currentTimeMillis();
                    initializeGambler(event, authorId, username, guildId, guildName, 0);
                    long endTime = System.currentTimeMillis();
                    long totalTime = endTime - startTime;
                    returnedItem = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
                    discordServer = new DiscordServer(guildId, returnedItem);
                    logger.info("Finished initialize player in " + totalTime + "ms for guild: " + guildName);
                } catch (Exception e) {
                    logger.info("Error initializing player for authorId {}, guildId {}", authorId, guildId);
                    logger.info(e.getMessage());
                    throw e;
                }
            }
            List<Map.Entry<String, AttributeValue>> sortedList = discordServer.topGamblers();

            String result = "";
            int counter = 1;
            for (Map.Entry<String, AttributeValue> entry : sortedList) {
                if (counter == 1) {
                    if (isReset) {
                        result += "Final Standings\n-----------------------\n";
                    } else {
                        result += "Leaderboard\n-----------------------\n";
                    }
                }
                String[] data = entry.getValue().s().split("-");
                result += counter + ". " + data[Constants.USERNAME] + "\t" + data[Constants.CURRENT_POINTS] + "\n";
                counter++;
            }
            MessageCreateRequest messageCreateRequest = MessageCreateRequest.builder().content(result).build();
            client.getChannelById(event.getInteraction().getChannelId()).createMessage(messageCreateRequest).block();
            return event.deleteReply();

        } catch (Exception e) {
            if (e.getMessage() != null && (e.getMessage().contains("50013") || e.getMessage().contains("50001"))){
                return event.editReply("I need permission to send messages in this channel.").then();
            }
            logger.info("Error getting leaderboard for, guildId {}", guildId);
            return event.editReply().withContent("Issue displaying leaderboard. Join our discord for assistance or to report bugs. https://discord.gg/hnj898AyKb").then();
        }
    }

    public Mono<Void> creator(ChatInputInteractionEvent event) {
        EmbedData embed = EmbedData.builder()
                .color(1)
                .title("About Me")
                .description("Hello! My name is Brendan and I am the solo developer of this bot. " +
                        "I created this bot in my spare time as something meant just for my friends and I, but others showed interest so I made it more widely available." +
                        "\n I am happy there are so many people who enjoy something I created and truly appreciate all the feedback." +
                        "If you would like to support me and keep this bot going, please rate the bot on [Top.gg](https://top.gg/bot/1232864977719922820)."
                )
                .timestamp(String.valueOf(Instant.now()))
                .footer(EmbedFooterData.builder().text("Created").build())
                .build();
        MessageCreateRequest messageCreateRequest = MessageCreateRequest.builder().addEmbed(embed).build();
        MessageData messageData = client.getChannelById(event.getInteraction().getChannelId()).createMessage(messageCreateRequest).block();
        return event.deleteReply();
    }

    static class ResetPointsTask extends TimerTask {
        private final DiscordClient client;

        public ResetPointsTask(DiscordClient client) {
            this.client = client;
        }

        @Override
        public void run() {
            System.out.println("Running scheduled task...");
            try {

                Map<String, AttributeValue> dailyResets = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, Constants.RESET_SCHEDULES_DAILY);
                Map<String, AttributeValue> monthlyResets = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, Constants.RESET_SCHEDULES_MONTHLY);
                Map<String, AttributeValue> mondayResets = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, Constants.RESET_SCHEDULES_MO);
                Map<String, AttributeValue> tuesdayResets = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, Constants.RESET_SCHEDULES_TU);
                Map<String, AttributeValue> wednesdayResets = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, Constants.RESET_SCHEDULES_WE);
                Map<String, AttributeValue> thursdayResets = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, Constants.RESET_SCHEDULES_TH);
                Map<String, AttributeValue> fridayResets = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, Constants.RESET_SCHEDULES_FR);
                Map<String, AttributeValue> saturdayResets = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, Constants.RESET_SCHEDULES_SAT);
                Map<String, AttributeValue> sundayResets = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, Constants.RESET_SCHEDULES_SUN);

                if (dailyResets.containsKey(Constants.GUILDS)) {
                    for (Map.Entry<String, AttributeValue> entry : dailyResets.get(Constants.GUILDS).m().entrySet()) {
                        String guildId = entry.getKey();
                        logger.info("Resetting guild id: " + guildId);
                        resetPointsWithRetries(guildId, 0);
                    }
                }

                //Check if it is the first of the month, if so reset monthly points
                LocalDate today = LocalDate.now(ZoneId.of("America/Chicago"));

                // Check if the day of the month is 1
                if (today.getDayOfMonth() == 1) {
                    if (monthlyResets.containsKey(Constants.GUILDS)) {
                        for (Map.Entry<String, AttributeValue> entry : monthlyResets.get(Constants.GUILDS).m().entrySet()) {
                            String guildId = entry.getKey();
                            logger.info("Resetting guild id: " + guildId);
                            resetPointsWithRetries(guildId, 0);
                        }
                    }
                }
                //Check the day of the week, for a given day of the week, reset those points
                if (today.getDayOfWeek() == DayOfWeek.MONDAY) {
                    if (mondayResets.containsKey(Constants.GUILDS)) {
                        for (Map.Entry<String, AttributeValue> entry : mondayResets.get(Constants.GUILDS).m().entrySet()) {
                            String guildId = entry.getKey();
                            logger.info("Resetting guild id: " + guildId);
                            resetPointsWithRetries(guildId, 0);
                        }
                    }
                }
                if (today.getDayOfWeek() == DayOfWeek.TUESDAY) {
                    if (tuesdayResets.containsKey(Constants.GUILDS)) {
                        for (Map.Entry<String, AttributeValue> entry : tuesdayResets.get(Constants.GUILDS).m().entrySet()) {
                            String guildId = entry.getKey();
                            logger.info("Resetting guild id: " + guildId);
                            resetPointsWithRetries(guildId, 0);
                        }
                    }
                }

                if (today.getDayOfWeek() == DayOfWeek.WEDNESDAY) {
                    if (wednesdayResets.containsKey(Constants.GUILDS)) {
                        for (Map.Entry<String, AttributeValue> entry : wednesdayResets.get(Constants.GUILDS).m().entrySet()) {
                            String guildId = entry.getKey();
                            logger.info("Resetting guild id: " + guildId);
                            resetPointsWithRetries(guildId, 0);
                        }
                    }
                }
                if (today.getDayOfWeek() == DayOfWeek.THURSDAY) {
                    if (thursdayResets.containsKey(Constants.GUILDS)) {
                        for (Map.Entry<String, AttributeValue> entry : thursdayResets.get(Constants.GUILDS).m().entrySet()) {
                            String guildId = entry.getKey();
                            logger.info("Resetting guild id: " + guildId);
                            resetPointsWithRetries(guildId, 0);
                        }
                    }
                }
                if (today.getDayOfWeek() == DayOfWeek.FRIDAY) {
                    if (fridayResets.containsKey(Constants.GUILDS)) {
                        for (Map.Entry<String, AttributeValue> entry : fridayResets.get(Constants.GUILDS).m().entrySet()) {
                            String guildId = entry.getKey();
                            logger.info("Resetting guild id: " + guildId);
                            resetPointsWithRetries(guildId, 0);
                        }
                    }
                }
                if (today.getDayOfWeek() == DayOfWeek.SATURDAY) {
                    if (saturdayResets.containsKey(Constants.GUILDS)) {
                        for (Map.Entry<String, AttributeValue> entry : saturdayResets.get(Constants.GUILDS).m().entrySet()) {
                            String guildId = entry.getKey();
                            logger.info("Resetting guild id: " + guildId);
                            resetPointsWithRetries(guildId, 0);
                        }
                    }
                }
                if (today.getDayOfWeek() == DayOfWeek.SUNDAY) {
                    if (sundayResets.containsKey(Constants.GUILDS)) {
                        for (Map.Entry<String, AttributeValue> entry : sundayResets.get(Constants.GUILDS).m().entrySet()) {
                            String guildId = entry.getKey();
                            logger.info("Resetting guild id: " + guildId);
                            resetPointsWithRetries(guildId, 0);
                        }
                    }
                }
                System.out.println("Finished scheduled task...");
            } catch (Exception e) {
                logger.info("Error running daily task for resetting points.");
                logger.info(e.getMessage());
            }
        }

        private static void resetPointsWithRetries(String guildId, int retries) throws Exception {
            try {
                Map<String, AttributeValue> returnedItem = dynamoDBHelper.getItemFromTable(Constants.GUILD_ID, guildId);
                DiscordServer discordServer = new DiscordServer(guildId, returnedItem);
                String startingPoints = "1000";
                if (discordServer.getStartingPoints() != null) {
                    startingPoints = discordServer.getStartingPoints();
                }
                resetPoints(guildId, discordServer.getModifiableUsers(), discordServer, startingPoints);

            } catch (ConditionalCheckFailedException e) {
                logger.info(e.getMessage());
                if (retries < 10) {
                    resetPointsWithRetries(guildId, retries + 1);
                } else {
                    throw (new Exception("Conditional Exception hit more than 10 times."));
                }

            }
        }
    }
}
