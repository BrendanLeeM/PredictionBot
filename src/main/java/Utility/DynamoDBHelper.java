package Utility;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class DynamoDBHelper {

    private static DynamoDbClient dynamoDbClient;
    private String tableName = System.getenv("GuildTable");

    public DynamoDBHelper(DynamoDbClient dynamoDbClient) {
        this.dynamoDbClient = dynamoDbClient;
    }


    public Map<String, AttributeValue> getItemFromTable(String key, String keyVal) {
        Map<String, AttributeValue> returnedItem;
        HashMap<String, AttributeValue> keyToGet = new HashMap<String, AttributeValue>();

        keyToGet.put(key, AttributeValue.builder()
                .s(keyVal).build());

        GetItemRequest request = GetItemRequest.builder()
                .key(keyToGet)
                .tableName(tableName)
                .build();

        try {
            returnedItem = dynamoDbClient.getItem(request).item();


        } catch (DynamoDbException e) {
            throw (e);
        }
        return returnedItem;
    }

    public void putItemInTable(Map<String, AttributeValue> itemMap) {
        itemMap.put(Constants.CREATE_DATE, AttributeValue.builder()
                .s(LocalDate.now().format(DateTimeFormatter.ISO_DATE))
                .build());
        PutItemRequest request = PutItemRequest.builder()
                .tableName(tableName)
                .item(itemMap)
                .build();

        try {
            dynamoDbClient.putItem(request);

        } catch (ResourceNotFoundException e) {
            System.err.format("Error: The Amazon DynamoDB table \"%s\" can't be found.\n", tableName);
            System.err.println("Be sure that it exists and that you've typed its name correctly!");
            throw (e);
        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            throw (e);
        }
    }

    public void updateTableItem(
            String guildId,
            HashMap<String, AttributeValue> updatedValues,
            HashSet<String> keys) {

        HashMap<String, AttributeValue> itemKey = new HashMap<>();
        itemKey.put(Constants.GUILD_ID, AttributeValue.builder()
                .s(guildId)
                .build());
        LocalDate currentDate = LocalDate.now();
        String isoDate = currentDate.format(DateTimeFormatter.ISO_DATE);
        updatedValues.put(":" + Constants.LAST_UPDATE, AttributeValue.builder()
                .s(isoDate)
                .build());
        keys.add(Constants.LAST_UPDATE);

        StringBuilder updateExpression = new StringBuilder("SET ");
        int mapSize = updatedValues.keySet().size();
        int count = 1;

        Map<String, String> attributeNames = new HashMap<>();
        String conditionExpression = "";
        boolean isFirstCondition = true;

        boolean shouldUseConditionals = true;
        if ((keys.contains(Constants.USERS_VERSION)
                && keys.contains(Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_ONE)
                && keys.contains(Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_TWO)
                && keys.contains(Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_THREE)
                && keys.contains(Constants.GAMBLERS_ONE_VERSION)
                && keys.contains(Constants.GAMBLERS_TWO_VERSION)
                && keys.contains(Constants.GAMBLERS_THREE_VERSION))){
            shouldUseConditionals = false;
        }

        for (String key : keys) {
            if (key.equalsIgnoreCase("Users")) {
                updateExpression.append("#usersKey = :" + key);
                attributeNames.put("#usersKey", key);
            } else if (key.equalsIgnoreCase("Role")) {
                updateExpression.append("#roleKey = :" + key);
                attributeNames.put("#roleKey", key);
            } else {
                updateExpression.append(key + " = :" + key);
            }
            //If we are updating everything, we don't care about versions so skip it
            if (shouldUseConditionals) {
                if (Constants.USERS_VERSION.equalsIgnoreCase(key)
                        || Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_ONE.equalsIgnoreCase((key))
                        || Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_TWO.equalsIgnoreCase((key))
                        || Constants.IN_PROGRESS_GAMBLE_DATA_VERSION_THREE.equalsIgnoreCase((key))
                        || Constants.GAMBLERS_ONE_VERSION.equalsIgnoreCase((key))
                        || Constants.GAMBLERS_TWO_VERSION.equalsIgnoreCase((key))
                        || Constants.GAMBLERS_THREE_VERSION.equalsIgnoreCase((key))
                        || Constants.GUILDS_VERSION.equalsIgnoreCase((key))) {
                    if (isFirstCondition) {
                        isFirstCondition = false;
                        conditionExpression = conditionExpression.concat("(" + key + " < :" + key + " OR attribute_not_exists(" + key + "))");
                    } else {
                        conditionExpression = conditionExpression.concat(" AND (" + key + " < :" + key + " OR attribute_not_exists(" + key + "))");
                    }

                }
            }
            if (count < mapSize) {
                updateExpression.append(", ");
            }
            count++;
        }
        UpdateItemRequest request;

        //IF WE HAVE A CONDITION
        if (!isFirstCondition && !attributeNames.isEmpty()) {
            request = UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(itemKey)
                    .updateExpression(updateExpression.toString())
                    .expressionAttributeNames(attributeNames)
                    .expressionAttributeValues(updatedValues)
                    .conditionExpression(conditionExpression)
                    .build();
        } else if (!attributeNames.isEmpty()) {
            request = UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(itemKey)
                    .updateExpression(updateExpression.toString())
                    .expressionAttributeNames(attributeNames)
                    .expressionAttributeValues(updatedValues)
                    .build();
        } else {
            request = UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(itemKey)
                    .updateExpression(updateExpression.toString())
                    .expressionAttributeValues(updatedValues)
                    .build();
        }


        try {
            dynamoDbClient.updateItem(request);
        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            throw (e);
        }
    }


}
