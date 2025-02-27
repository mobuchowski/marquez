package marquez.graphql;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import graphql.ExecutionResult;
import graphql.GraphQL;
import io.dropwizard.util.Resources;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import marquez.IntegrationTests;
import marquez.JdbiRuleInit;
import marquez.common.Utils;
import marquez.db.OpenLineageDao;
import marquez.service.OpenLineageService;
import marquez.service.RunService;
import marquez.service.models.LineageEvent;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.testing.JdbiRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({IntegrationTests.class})
public class LineageTest {
  @ClassRule public static final JdbiRule dbRule = JdbiRuleInit.init();
  private static GraphQL graphQL;

  @BeforeClass
  public static void setup() throws IOException, ExecutionException, InterruptedException {
    Jdbi jdbi = dbRule.getJdbi();
    GraphqlSchemaBuilder schemaBuilder = new GraphqlSchemaBuilder(jdbi);
    graphQL = GraphQL.newGraphQL(schemaBuilder.buildSchema()).build();
    OpenLineageDao openLineageDao = jdbi.onDemand(OpenLineageDao.class);
    List<String> events =
        ImmutableList.of(
            "1_pick_apples.json",
            "2_apple_filling.json",
            "3_apple_cider.json",
            "5_apple_vinegar.json",
            "6_apple_pie.json",
            "7_pick_different_apples.json",
            "8_apple_filling.json",
            "9_apple_pie.json");

    for (String event : events) {
      LineageEvent lineageEvent =
          Utils.newObjectMapper()
              .readValue(
                  Resources.getResource(String.format("apples/%s", event)), LineageEvent.class);

      OpenLineageService service = new OpenLineageService(openLineageDao, mock(RunService.class));
      service.createAsync(lineageEvent).get();
    }
  }

  @Test
  public void testGraphql() {
    ExecutionResult result =
        graphQL.execute(
            ""
                + "{\n"
                + "  lineageFromJob(\n"
                + "      name:\"prepare_apple_filling\", \n"
                + "      namespace:\"grandmas.kitchen\",\n"
                + "      depth:10){\n"
                + "    graph {\n"
                + "      ... on DatasetLineageEntry {\n"
                + "        name\n"
                + "        namespace\n"
                + "        type\n"
                + "        data {\n"
                + "          name\n"
                + "          physicalName\n"
                + "          fields {\n"
                + "            name\n"
                + "          }\n"
                + "        }\n"
                + "        inEdges {\n"
                + "          name\n"
                + "          namespace\n"
                + "          type\n"
                + "        }\n"
                + "        outEdges {\n"
                + "          name\n"
                + "          namespace\n"
                + "          type\n"
                + "        }\n"
                + "      }\n"
                + "      ... on JobLineageEntry {\n"
                + "        name\n"
                + "        namespace\n"
                + "        type\n"
                + "        data {\n"
                + "          name\n"
                + "          currentVersion {\n"
                + "            version\n"
                + "            location\n"
                + "          }\n"
                + "        }\n"
                + "        inEdges {\n"
                + "          name\n"
                + "          namespace\n"
                + "\n"
                + "        }\n"
                + "        outEdges {\n"
                + "          name\n"
                + "          namespace\n"
                + "\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}");

    assertTrue(result.getErrors().isEmpty());
  }
}
