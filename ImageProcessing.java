

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;

import javax.imageio.ImageIO;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;

public class ImageProcessing {

		public static final String DIRECTORY = "photos/";
		private final AmazonS3Client amazonS3Client;
		private final String bucketName = "perenc-lab4";

		public ImageProcessing(AWSCredentials credentials) {
			this.amazonS3Client = new AmazonS3Client(credentials);
		}

		public void rotateImage(String imageName) {
	        S3Object object = amazonS3Client.getObject(bucketName, imageName);
	        try {
	            byte[] bytes = IOUtils.toByteArray(object.getObjectContent());
	            InputStream in = new ByteArrayInputStream(bytes);
	            BufferedImage bImageFromConvert = ImageIO.read(in);
	            BufferedImage newImage = new BufferedImage(bImageFromConvert.getHeight(), bImageFromConvert.getWidth(), bImageFromConvert.getType());
	            Graphics2D graphics = (Graphics2D) newImage.getGraphics();
	            graphics.rotate(Math.toRadians(90), newImage.getWidth() / 2, newImage.getHeight() / 2);
	            graphics.translate((newImage.getWidth() - bImageFromConvert.getWidth()) / 2, (newImage.getHeight() - bImageFromConvert.getHeight()) / 2);
	            graphics.drawImage(bImageFromConvert, 0, 0, bImageFromConvert.getWidth(), bImageFromConvert.getHeight(), null);
	            ByteArrayOutputStream baos = new ByteArrayOutputStream();

	            try {
	                ImageIO.write(newImage, "jpg", baos);
	                baos.flush();
	            } catch (IOException e1) {
	                e1.printStackTrace();
	            }


	            byte[] buf = baos.toByteArray();
	            InputStream str = new ByteArrayInputStream(buf);
	            ObjectMetadata metadata2 = new ObjectMetadata();
	            metadata2.setContentLength(buf.length);
	            amazonS3Client.putObject(bucketName, imageName.substring(0,imageName.length()-3)+ LocalDateTime.now() + ".jpg", str, metadata2);

	        } catch (IOException e) {
	        	e.printStackTrace();
	        }
	        System.out.println("Zdjecie zostalo zapisane");

	    }
	}

