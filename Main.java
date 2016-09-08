
public class Main {

    public static void main(String[] args) {
        final Thread listener = new Thread(new Runnable() {
            public void run() {
            	System.out.println("Sprawdzanie kolejki rozpoczête.");
            	SQSmanager sqs_manager= new SQSmanager();
                try {
                	sqs_manager.listen();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        listener.start();
    }
}
