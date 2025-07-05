package Model;

import Utility.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.*;

public class DiscordServer {
    String guildId;
    GambleData predictionOne;
    GambleData predictionTwo;
    GambleData predictionThree;
    GambleData previousPredictionOne;
    GambleData previousPredictionTwo;
    GambleData previousPredictionThree;
    String role;
    String dailyPoints;
    String startingPoints;
    String resetSchedule;
    int usersVersion = 0;
    int inProgressGambleOneVersion = 0;
    int inProgressGambleTwoVersion = 0;
    int inProgressGambleThreeVersion = 0;
    int gamblersOneVersion = 0;
    int gamblersTwoVersion = 0;
    int gamblersThreeVersion = 0;

    Map<String, AttributeValue> users;
    Map<String, AttributeValue> gamblersOne;
    Map<String, AttributeValue> gamblersTwo;
    Map<String, AttributeValue> gamblersThree;
    Map<String, AttributeValue> previousGamblersOne;
    Map<String, AttributeValue> previousGamblersTwo;
    Map<String, AttributeValue> previousGamblersThree;

    Boolean initialized;

    private static Logger logger = LogManager.getLogger(DiscordServer.class);

    public DiscordServer(String guildId, Map<String, AttributeValue> returnedItem) {
        if (!returnedItem.isEmpty()) {

            this.guildId = guildId;

            if (returnedItem.containsKey(Constants.IN_PROGRESS_GAMBLE_DATA_ONE)) {
                predictionOne = new GambleData();
                if (Constants.NO_SESSION.equalsIgnoreCase(returnedItem.get(Constants.IN_PROGRESS_GAMBLE_DATA_ONE).s())) {
                    predictionOne.setTitle(Constants.NO_SESSION);
                } else {
                    String[] data = returnedItem.get(Constants.IN_PROGRESS_GAMBLE_DATA_ONE).s().split("-");
                    predictionOne.setTitle(data[Constants.IN_PROGRESS_GAMBLE]);
                    predictionOne.setOption1(data[Constants.OPTION_1]);
                    predictionOne.setOption2(data[Constants.OPTION_2]);
                    predictionOne.setTimer(data[Constants.TIMER]);
                    predictionOne.setOption1Pot(data[Constants.OPTION_1_POT]);
                    predictionOne.setOption2Pot(data[Constants.OPTION_2_POT]);
                    predictionOne.setMessageId(data[Constants.MESSAGE_ID]);
                }
            } else {
                predictionOne = new GambleData();
                predictionOne.setTitle(Constants.NO_SESSION);
            }

            if (returnedItem.containsKey(Constants.IN_PROGRESS_GAMBLE_DATA_TWO)) {
                predictionTwo = new GambleData();
                if (Constants.NO_SESSION.equalsIgnoreCase(returnedItem.get(Constants.IN_PROGRESS_GAMBLE_DATA_TWO).s())) {
                    predictionTwo.setTitle(Constants.NO_SESSION);
                } else {
                    String[] data = returnedItem.get(Constants.IN_PROGRESS_GAMBLE_DATA_TWO).s().split("-");
                    predictionTwo.setTitle(data[Constants.IN_PROGRESS_GAMBLE]);
                    predictionTwo.setOption1(data[Constants.OPTION_1]);
                    predictionTwo.setOption2(data[Constants.OPTION_2]);
                    predictionTwo.setTimer(data[Constants.TIMER]);
                    predictionTwo.setOption1Pot(data[Constants.OPTION_1_POT]);
                    predictionTwo.setOption2Pot(data[Constants.OPTION_2_POT]);
                    predictionTwo.setMessageId(data[Constants.MESSAGE_ID]);
                }
            } else {
                predictionTwo = new GambleData();
                predictionTwo.setTitle(Constants.NO_SESSION);
            }

            if (returnedItem.containsKey(Constants.IN_PROGRESS_GAMBLE_DATA_THREE)) {
                predictionThree = new GambleData();
                if (Constants.NO_SESSION.equalsIgnoreCase(returnedItem.get(Constants.IN_PROGRESS_GAMBLE_DATA_THREE).s())) {
                    predictionThree.setTitle(Constants.NO_SESSION);
                } else {
                    String[] data = returnedItem.get(Constants.IN_PROGRESS_GAMBLE_DATA_THREE).s().split("-");
                    predictionThree.setTitle(data[Constants.IN_PROGRESS_GAMBLE]);
                    predictionThree.setOption1(data[Constants.OPTION_1]);
                    predictionThree.setOption2(data[Constants.OPTION_2]);
                    predictionThree.setTimer(data[Constants.TIMER]);
                    predictionThree.setOption1Pot(data[Constants.OPTION_1_POT]);
                    predictionThree.setOption2Pot(data[Constants.OPTION_2_POT]);
                    predictionThree.setMessageId(data[Constants.MESSAGE_ID]);
                }
            } else {
                predictionThree = new GambleData();
                predictionThree.setTitle(Constants.NO_SESSION);
            }

            if (returnedItem.containsKey(Constants.PREVIOUS_PREDICTION_ONE)) {
                previousPredictionOne = new GambleData();
                if (!returnedItem.get(Constants.PREVIOUS_PREDICTION_ONE).s().isEmpty()) {
                    String[] data = returnedItem.get(Constants.PREVIOUS_PREDICTION_ONE).s().split("-");
                    previousPredictionOne.setTitle(data[Constants.IN_PROGRESS_GAMBLE]);
                    previousPredictionOne.setOption1(data[Constants.OPTION_1]);
                    previousPredictionOne.setOption2(data[Constants.OPTION_2]);
                    previousPredictionOne.setTimer(data[Constants.TIMER]);
                    previousPredictionOne.setOption1Pot(data[Constants.OPTION_1_POT]);
                    previousPredictionOne.setOption2Pot(data[Constants.OPTION_2_POT]);
                    previousPredictionOne.setMessageId(data[Constants.MESSAGE_ID]);
                }
            }

            if (returnedItem.containsKey(Constants.PREVIOUS_PREDICTION_TWO)) {
                previousPredictionTwo = new GambleData();
                if (!returnedItem.get(Constants.PREVIOUS_PREDICTION_TWO).s().isEmpty()) {
                    String[] data = returnedItem.get(Constants.PREVIOUS_PREDICTION_TWO).s().split("-");
                    previousPredictionTwo.setTitle(data[Constants.IN_PROGRESS_GAMBLE]);
                    previousPredictionTwo.setOption1(data[Constants.OPTION_1]);
                    previousPredictionTwo.setOption2(data[Constants.OPTION_2]);
                    previousPredictionTwo.setTimer(data[Constants.TIMER]);
                    previousPredictionTwo.setOption1Pot(data[Constants.OPTION_1_POT]);
                    previousPredictionTwo.setOption2Pot(data[Constants.OPTION_2_POT]);
                    previousPredictionTwo.setMessageId(data[Constants.MESSAGE_ID]);
                }
            }

            if (returnedItem.containsKey(Constants.PREVIOUS_PREDICTION_THREE)) {
                previousPredictionThree = new GambleData();
                if (!returnedItem.get(Constants.PREVIOUS_PREDICTION_THREE).s().isEmpty()) {
                    String[] data = returnedItem.get(Constants.PREVIOUS_PREDICTION_THREE).s().split("-");
                    previousPredictionThree.setTitle(data[Constants.IN_PROGRESS_GAMBLE]);
                    previousPredictionThree.setOption1(data[Constants.OPTION_1]);
                    previousPredictionThree.setOption2(data[Constants.OPTION_2]);
                    previousPredictionThree.setTimer(data[Constants.TIMER]);
                    previousPredictionThree.setOption1Pot(data[Constants.OPTION_1_POT]);
                    previousPredictionThree.setOption2Pot(data[Constants.OPTION_2_POT]);
                    previousPredictionThree.setMessageId(data[Constants.MESSAGE_ID]);
                }
            }

            if (returnedItem.containsKey(Constants.ROLE)) {
                role = returnedItem.get(Constants.ROLE).s();
            }

            if (returnedItem.containsKey(Constants.DAILY_POINTS)) {
                dailyPoints = returnedItem.get(Constants.DAILY_POINTS).s();
            }

            if (returnedItem.containsKey(Constants.STARTING_POINTS)) {
                startingPoints = returnedItem.get(Constants.STARTING_POINTS).s();
            }

            if (returnedItem.containsKey(Constants.RESET_SCHEDULE)) {
                resetSchedule = returnedItem.get(Constants.RESET_SCHEDULE).s();
            }

            if (returnedItem.containsKey(Constants.USERS)) {
                users = returnedItem.get(Constants.USERS).m();
            }

            if (returnedItem.containsKey(Constants.USERS_VERSION)) {
                usersVersion = Integer.parseInt(returnedItem.get(Constants.USERS_VERSION).n());
            }
            if (returnedItem.containsKey(Constants.GAMBLERS_ONE_VERSION)) {
                gamblersOneVersion = Integer.parseInt(returnedItem.get(Constants.GAMBLERS_ONE_VERSION).n());
            }
            if (returnedItem.containsKey(Constants.GAMBLERS_TWO_VERSION)) {
                gamblersTwoVersion = Integer.parseInt(returnedItem.get(Constants.GAMBLERS_TWO_VERSION).n());
            }
            if (returnedItem.containsKey(Constants.GAMBLERS_THREE_VERSION)) {
                gamblersThreeVersion = Integer.parseInt(returnedItem.get(Constants.GAMBLERS_THREE_VERSION).n());
            }
            if (returnedItem.containsKey(Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_ONE)) {
                inProgressGambleOneVersion = Integer.parseInt(returnedItem.get(Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_ONE).n());
            }
            if (returnedItem.containsKey(Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_TWO)) {
                inProgressGambleTwoVersion = Integer.parseInt(returnedItem.get(Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_TWO).n());
            }
            if (returnedItem.containsKey(Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_THREE)) {
                inProgressGambleThreeVersion = Integer.parseInt(returnedItem.get(Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_THREE).n());
            }
            if (returnedItem.containsKey(Constants.GAMBLERS_ONE)) {
                gamblersOne = returnedItem.get(Constants.GAMBLERS_ONE).m();
            }
            if (returnedItem.containsKey(Constants.GAMBLERS_TWO)) {
                gamblersTwo = returnedItem.get(Constants.GAMBLERS_TWO).m();
            }
            if (returnedItem.containsKey(Constants.GAMBLERS_THREE)) {
                gamblersThree = returnedItem.get(Constants.GAMBLERS_THREE).m();
            }
            if (returnedItem.containsKey(Constants.PREVIOUS_GAMBLERS_ONE)) {
                previousGamblersOne = returnedItem.get(Constants.PREVIOUS_GAMBLERS_ONE).m();
            }
            if (returnedItem.containsKey(Constants.PREVIOUS_GAMBLERS_TWO)) {
                previousGamblersTwo = returnedItem.get(Constants.PREVIOUS_GAMBLERS_TWO).m();
            }
            if (returnedItem.containsKey(Constants.PREVIOUS_GAMBLERS_THREE)) {
                previousGamblersThree = returnedItem.get(Constants.PREVIOUS_GAMBLERS_THREE).m();
            }

            initialized = true;
        } else {
            initialized = false;
        }
    }

    public void setPreviousGamblersOne(Map<String, AttributeValue> previousGamblersOne) {
        this.previousGamblersOne = previousGamblersOne;
    }


    public GambleData getPredictionOne() {
        return predictionOne;
    }

    public GambleData getPredictionTwo() {
        return predictionTwo;
    }

    public GambleData getPredictionThree() {
        return predictionThree;
    }

    public String getRole() {
        return role;
    }

    public String getStartingPoints() {
        return startingPoints;
    }

    public int getUsersVersion() {
        return usersVersion;
    }

    public String getDailyPoints() {
        return dailyPoints;
    }

    public int getGamblersOneVersion() {
        return gamblersOneVersion;
    }

    public int getInProgressGambleOneVersion() {
        return inProgressGambleOneVersion;
    }

    public int getInProgressGambleTwoVersion() {
        return inProgressGambleTwoVersion;
    }

    public int getGamblersTwoVersion() {
        return gamblersTwoVersion;
    }

    public int getGamblersThreeVersion() {
        return gamblersThreeVersion;
    }

    public Map<String, AttributeValue> getPreviousGamblersTwo() {
        return previousGamblersTwo;
    }

    public Map<String, AttributeValue> getPreviousGamblersThree() {
        return previousGamblersThree;
    }

    public int getInProgressGambleThreeVersion() {
        return inProgressGambleThreeVersion;
    }

    public Map<String, AttributeValue> getUsers() {
        return users;
    }

    public String getResetSchedule() {
        return resetSchedule;
    }

    public void setResetSchedule(String resetSchedule) {
        this.resetSchedule = resetSchedule;
    }

    public Map<String, AttributeValue> getGamblersOne() {
        return gamblersOne;
    }

    public Map<String, AttributeValue> getGamblersTwo() {
        return gamblersTwo;
    }

    public Map<String, AttributeValue> getGamblersThree() {
        return gamblersThree;
    }

    public Map<String, AttributeValue> getPreviousGamblersOne() {
        return previousGamblersOne;
    }

    public Map<String, AttributeValue> getModifiableUsers() {
        Map<String, AttributeValue> newUserMap = new HashMap<>();
        if (users == null) {
            return new HashMap<>();
        }
        for (Map.Entry<String, AttributeValue> user : users.entrySet()) {
            newUserMap.put(user.getKey(), user.getValue());
        }
        return newUserMap;
    }

    public GambleData getPreviousPredictionOne() {
        return previousPredictionOne;
    }

    public GambleData getPreviousPredictionTwo() {
        return previousPredictionTwo;
    }

    public GambleData getPreviousPredictionThree() {
        return previousPredictionThree;
    }

    public Map<String, AttributeValue> getModifiableGamblers(Map<String, AttributeValue> gamblers) {
        Map<String, AttributeValue> newUserMap = new HashMap<>();
        if (gamblers == null) {
            return new HashMap<>();
        }
        for (Map.Entry<String, AttributeValue> user : gamblers.entrySet()) {
            newUserMap.put(user.getKey(), user.getValue());
        }
        return newUserMap;
    }

    public boolean userExists(String authorId) {
        return users != null && !users.isEmpty() && users.containsKey(authorId);
    }

    public boolean gamblerExists(String authorId) {
        return gamblersOne != null && !gamblersOne.isEmpty() && gamblersOne.containsKey(authorId);
    }

    public boolean hasServerBeenInitialized() {
        return initialized;
    }

    public String getCurrentPoints(String authorId) throws Exception {
        if (userExists(authorId) && users.get(authorId).s().split("-").length == 3) {
            return users.get(authorId).s().split("-")[Constants.CURRENT_POINTS];
        } else {
            logger.debug("Unable to get points for guildId {} and authorId {}", guildId, authorId);
            throw new Exception("Unable to retrieve points at this moment.");
        }
    }

    public boolean hasUsernameChanged(String authorId, String username) throws Exception {
        String[] userData = users.get(authorId).s().split("-");
        if (userData.length != 3) {
            throw new Exception("Issue when parsing username for user: " + authorId);
        }
        return !username.equalsIgnoreCase(userData[Constants.USERNAME]);
    }

    public boolean isGambleOneInProgress() {
        if (!initialized) {
            return false;
        }
        return !Constants.NO_SESSION.equalsIgnoreCase(predictionOne.getTitle());
    }

    public boolean isGambleTwoInProgress() {
        if (!initialized) {
            return false;
        }
        return !Constants.NO_SESSION.equalsIgnoreCase(predictionTwo.getTitle());
    }

    public boolean isGambleThreeInProgress() {
        if (!initialized) {
            return false;
        }
        return !Constants.NO_SESSION.equalsIgnoreCase(predictionThree.getTitle());
    }

    public boolean optionMatches(String option) {
        return option.equalsIgnoreCase(predictionOne.getOption1()) || option.equalsIgnoreCase(predictionOne.getOption2());
    }

    public String getUsername(String authorId) throws Exception {
        String[] userData = users.get(authorId).s().split("-");
        if (userData.length != 3) {
            throw new Exception("Issue when parsing username for user: " + authorId);
        }
        return userData[Constants.USERNAME];
    }

    public String getDaily(String authorId) throws Exception {
        if (userExists(authorId)) {
            String[] data = users.get(authorId).s().split("-");
            if (data.length != 3) {
                logger.debug("Invalid data in database for user: {}", authorId);
                throw new Exception("Issue retrieving daily points.");
            } else {
                return data[Constants.DAILY];
            }
        }
        return "Daily not available";
    }

    public List<Map.Entry<String, AttributeValue>> topGamblers() {
        List<Map.Entry<String, AttributeValue>> listToReturn;

        listToReturn = new LinkedList<Map.Entry<String, AttributeValue>>(getUsers().entrySet());

        Collections.sort(listToReturn, new Comparator<Map.Entry<String, AttributeValue>>() {
            @Override
            public int compare(Map.Entry<String, AttributeValue> o1, Map.Entry<String, AttributeValue> o2) {
                if (Integer.parseInt(o1.getValue().s().split("-")[0]) < Integer.parseInt(o2.getValue().s().split("-")[0])) {
                    return 1;
                }
                if (Integer.parseInt(o1.getValue().s().split("-")[0]) > Integer.parseInt(o2.getValue().s().split("-")[0])) {
                    return -1;
                }

                return 0;
            }
        });


        return listToReturn;
    }
}
