package Model;

import Utility.Constants;

public class User {
    String points;
    String username;
    String lastDailyPoints;

    public User (String data){
        String [] dataArray = data.split("-");
        points = dataArray[0];
        username = dataArray[1];
        lastDailyPoints = dataArray[2];
    }

    public String getUserAsString(){
        return points.concat("-").concat(username).concat("-").concat(lastDailyPoints);
    }

    public String getPoints() {
        return points;
    }

    public void setPoints(String points) {
        this.points = points;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getLastDailyPoints() {
        return lastDailyPoints;
    }

    public void setLastDailyPoints(String lastDailyPoints) {
        this.lastDailyPoints = lastDailyPoints;
    }
}
