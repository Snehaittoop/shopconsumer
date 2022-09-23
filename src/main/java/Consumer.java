import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;
import org.json.JSONException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        KafkaConsumer consumer;
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG,"consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put("group.id", "test");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("Demonest1"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {


                String fetchedValue = record.value();

                System.out.println(record.value());

                JSONObject jsonObject = new JSONObject(fetchedValue);
                System.out.println(String.valueOf(jsonObject));
                String getModelname = String.valueOf(jsonObject.getString("modelname"));
                String getbrand = String.valueOf(jsonObject.getString("brand"));
                String getreleseYear = String.valueOf(jsonObject.getInt("relese_year"));
                String getprice = String.valueOf(jsonObject.getInt("price"));
                String getsellerName = String.valueOf(jsonObject.getString("sellerName"));
                String getcolors = String.valueOf(jsonObject.getString("colors"));
                String getDate = String.valueOf((jsonObject.getInt("manufacturingDate")));
                System.out.println(getsellerName);
                System.out.println(getbrand);
                System.out.println(getreleseYear);
                System.out.println(getprice);
                System.out.println(getModelname);
                System.out.println(getcolors);
                System.out.println(getDate);
                try {


                    Class.forName("com.mysql.cj.jdbc.Driver");
                    conn = (Connection) DriverManager.getConnection("jdbc:mysql://localhost:3306/vechapp", "root", "");
                    stmt = (Statement) conn.createStatement();
                    String qry = "INSERT INTO `vehicle`( `modelname`, `brand`, `relese_year`, `price`, `sellerName`, `colors`, `manufacturingDate`) VALUES ('" + getModelname + "','" + getbrand + "','" + getreleseYear + "','" + getprice + "','" + getDate + "','" + getcolors + "','" + getsellerName + "')";
                    stmt.executeUpdate(qry);
                    System.out.println(qry);
                    System.out.println("succesfully created");
                } catch (Exception e) {
                    System.out.println("check connection"+e);

                }


            }
        }
    }
}
