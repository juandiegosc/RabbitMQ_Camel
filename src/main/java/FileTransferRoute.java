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
        String exchangeName = "integracion.exchange";
        String queueName = "integracion.pedidos.creados";

        // Endpoint del Productor (Envía al Exchange con un enrutamiento a nuestra cola)
        String producerEndpoint = "spring-rabbitmq:" + exchangeName
                + "?routingKey=" + queueName
                + "&autoDeclare=true"
                + "&connectionFactory=#rabbitConnectionFactory";

        // Endpoint del Consumidor (Escucha de la cola explícita)
        String consumerEndpoint = "spring-rabbitmq:" + exchangeName
                + "?queues=" + queueName
                + "&routingKey=" + queueName
                + "&autoDeclare=true"
                + "&connectionFactory=#rabbitConnectionFactory";

        from("file:input")
                .log("Detectado archivo: ${file:name} a las ${date:now:yyyy-MM-dd HH:mm:ss}")
                .setHeader("NombreOriginal", simple("${file:name}")) // Preservamos el nombre explícitamente
                .convertBodyTo(String.class)
                .to(producerEndpoint)
                .log("Mensaje publicado en RabbitMQ (Queue: " + queueName + ")");

        from(consumerEndpoint)
                .delay(5000) // Retraso de 5 segundos para poder ver el mensaje encolado/unacked en la consola de RabbitMQ
                .log("Mensaje procesado y recibido desde RabbitMQ (Queue: " + queueName + ")")
                .setHeader(org.apache.camel.Exchange.FILE_NAME, simple("${header.NombreOriginal}")) // Restauramos el nombreOriginal
                .to("file:output");
    }
}
