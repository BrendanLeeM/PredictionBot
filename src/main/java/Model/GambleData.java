package Model;

public class GambleData {
    String title;
    String option1;
    String option2;
    String timer;
    String option1Pot;
    String option2Pot;
    String messageId;

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getOption1Pot() {
        return option1Pot;
    }

    public void setOption1Pot(String option1Pot) {
        this.option1Pot = option1Pot;
    }

    public String getOption2Pot() {
        return option2Pot;
    }

    public void setOption2Pot(String option2Pot) {
        this.option2Pot = option2Pot;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getOption1() {
        return option1;
    }

    public void setOption1(String option1) {
        this.option1 = option1;
    }

    public String getOption2() {
        return option2;
    }

    public void setOption2(String option2) {
        this.option2 = option2;
    }

    public String getTimer() {
        return timer;
    }

    public void setTimer(String timer) {
        this.timer = timer;
    }
    @Override
    public String toString(){
        return title + "-" + option1 + "-" + option2 + "-"
                + timer + "-" + option1Pot + "-" + option2Pot + "-" + messageId;
    }
}
