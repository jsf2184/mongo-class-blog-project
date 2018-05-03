package simple.drivers;

import freemarker.template.Configuration;
import freemarker.template.Template;
import spark.Request;
import spark.Response;
import spark.Route;
import spark.Spark;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;


public class HelloWorldSparkFreemarkerStyle {
    public static void main( String[] args )
    {
        final Configuration configuration = new Configuration();
        configuration.setClassForTemplateLoading(HelloWorldSparkFreemarkerStyle.class, "/");

        // the spark.get() makes them HTTP GET handlers
        Spark.get(new Route("/") {
            @Override
            public Object handle(Request request, Response response) {
                StringWriter writer = new StringWriter();
                try {
                    Template helloTemplate = configuration.getTemplate("hello.ftl");
                    Map<String, Object> helloMap = new HashMap<String, Object>();
                    helloMap.put("name", "Jeffrey");
                    helloTemplate.process(helloMap, writer);

                } catch (Exception e) {
                    halt(500);
                    e.printStackTrace();
                }
                return writer.toString();

            }
        });

        Spark.get(new Route("/echo/:thing") {

            public Object handle(Request request, Response response) {
                String param = request.params(":thing");
                return param;
            }
        } );

    }

}
