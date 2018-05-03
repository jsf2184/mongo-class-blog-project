package simple.drivers;

import freemarker.template.Configuration;
import freemarker.template.Template;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;


public class HelloWorldFreemarkerStyle {
    public static void main( String[] args )
    {
        Configuration configuration = new Configuration();
        configuration.setClassForTemplateLoading(HelloWorldFreemarkerStyle.class, "/");
        try {
            Template helloTemplate = configuration.getTemplate("hello.ftl");
            StringWriter writer = new StringWriter();
            Map<String, Object> helloMap = new HashMap<String, Object>();
            helloMap.put("name", "Jeff");
            helloTemplate.process(helloMap, writer);
            System.out.println(writer.toString());

        } catch (Exception e) {
            e.printStackTrace();
        }

//        Spark.get(new Route("/") {
//            @Override
//            public Object handle(Request request, Response response) {
//                return "Hello World from Spark";
//            }
//        });
    }

}
