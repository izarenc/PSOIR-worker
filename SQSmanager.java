
import java.util.ArrayList;
import java.util.List;
import java.util.Date;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

public class SQSmanager {

	public static final String domainName = "perenc_PROJEKT";
	public static final String DATE = "Date";
	private static final String ITEM_NAME = "Item_Name";
	public static final String HTTPS_SQS_US_WEST_2_AMAZONAWS_COM = "https://sqs.us-west-2.amazonaws.com/983680736795/perencSQS";
	public static final String ITEM = "Item";
	private final String queName;
	private final AmazonSQSClient sqs;
	private final ImageProcessing imageProcesor;
	private final AmazonSimpleDBClient simpleDBClinet;

	public SQSmanager() {
		AWSCredentials credentials = new BasicAWSCredentials("",
				"");
		this.queName = "perencSQS";
		
		this.sqs = new AmazonSQSClient(credentials);
		this.sqs.setEndpoint(HTTPS_SQS_US_WEST_2_AMAZONAWS_COM);
		this.imageProcesor = new ImageProcessing(credentials);
		this.simpleDBClinet = new AmazonSimpleDBClient(credentials);
		this.simpleDBClinet.createDomain(new CreateDomainRequest(domainName));
	}

	public void listen() throws InterruptedException {
		while (true) {
			System.out.println("Oczekuje na nowe komunikaty");
			List<Message> messagesFromQueue = getMessagesFromQueue(getQueueUrl(this.queName));
			if (messagesFromQueue.size() > 0) {
				Message message = messagesFromQueue.get(0);
				deleteMessageFromQueue(getQueueUrl(this.queName), message);
				List<ReplaceableAttribute> attributes = new ArrayList<>();
				attributes.add(new ReplaceableAttribute().withName(ITEM).withValue(message.getBody()));
				attributes.add(new ReplaceableAttribute().withName(DATE).withValue((new Date()).toString()));
				simpleDBClinet.putAttributes(new PutAttributesRequest(domainName, ITEM_NAME, attributes));
				imageProcesor.rotateImage(message.getBody());
			} else {
				Thread.sleep(2000);
			}
		}
	}

	private List<Message> getMessagesFromQueue(String queueUrl) {
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
		List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
		return messages;

	}

	private String getQueueUrl(String queueName) {
		GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(queueName);
		return this.sqs.getQueueUrl(getQueueUrlRequest).getQueueUrl();
	}

	private void deleteMessageFromQueue(String queueUrl, Message message) {
		String messageRecieptHandle = message.getReceiptHandle();
		System.out.println("Wiadomosc usunieta : " + message.getBody());
		sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageRecieptHandle));

	}

}

