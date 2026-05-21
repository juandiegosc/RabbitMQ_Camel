import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.main.Main;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;

public class FileTransferRoute extends RouteBuilder {
    public static void main(String[] args) throws Exception {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");
        connectionFactory.setPort(5672);
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");

        Main main = new Main();
        main.bind("rabbitConnectionFactory", connectionFactory);
        main.configure().addRoutesBuilder(new FileTransferRoute());
        main.run();
    }

    @Override
    public void configure() throws Exception {
        String queueName = "integracion.pedidos.creados";
        String rabbitEndpoint = "spring-rabbitmq:" + queueName
                + "?queue=" + queueName
                + "&routingKey=" + queueName
                + "&exchangeType=direct"
                + "&autoDelete=false"
                + "&durable=true"
                + "&autoDeclare=true"
                + "&connectionFactory=#rabbitConnectionFactory";

        from("file:input?noop=true")
                .log("Detectado archivo: ${file:name} a las ${date:now:yyyy-MM-dd HH:mm:ss}")
                .convertBodyTo(String.class)
                .to(rabbitEndpoint)
                .log("Mensaje publicado en RabbitMQ: " + queueName);

        from(rabbitEndpoint)
                .log("Mensaje recibido desde RabbitMQ: " + queueName)
                .to("file:output");
    }
}
