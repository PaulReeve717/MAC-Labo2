package ch.heig.mac;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.driver.*;

public class Requests {
    private static final  Logger LOGGER = Logger.getLogger(Requests.class.getName());
    private final Driver driver;

    public Requests(Driver driver) {
        this.driver = driver;
    }

    public List<String> getDbLabels() {
        var dbVisualizationQuery = "CALL db.labels";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list(t -> t.get("label").asString());
        }
    }

    public List<Record> possibleSpreaders() {
        var dbVisualizationQuery =
                "match(sick:Person)-[v1:VISITS]->(place:Place)<-[v2:VISITS]-(healthy:Person)\n" +
                "where sick.healthstatus = 'Sick' and healthy.healthstatus = 'Healthy' \n" +
                "    and v1.starttime > sick.confirmedtime and v2.starttime > sick.confirmedtime\n" +
                "return distinct  sick.name as sickName";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
    }

    public List<Record> possibleSpreadCounts() {
        var dbVisualizationQuery =
                "match(sick:Person)-[v1:VISITS]->(place:Place)<-[v2:VISITS]-(healthy:Person)\n" +
                "where sick.healthstatus = 'Sick' and healthy.healthstatus = 'Healthy' \n" +
                "    and v1.starttime > sick.confirmedtime and v2.starttime > sick.confirmedtime\n" +
                "return distinct  sick.name as sickName, count(healthy) as nbHealthy";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
    }

    public List<Record> carelessPeople() {
        var dbVisualizationQuery =
                "match(sick:Person)-[visit:VISITS]->(place:Place)\n" +
                "with sick, count(place.id) as nbPlaces\n" +
                "where sick.healthstatus = 'Sick' and nbPlaces > 10\n" +
                "return distinct  sick.name as sickName, nbPlaces \n" +
                "order by nbPlaces desc";
        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
    }

    public List<Record> sociallyCareful() {
        var dbVisualizationQuery =
                "match (person:Person)-[v:VISITS]->(place:Place)\n" +
                "where  person.healthstatus = 'Sick' and not (person.confirmedtime < v.endtime and place.type = 'Bar') \n" +
                "return distinct person.name as sickName";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
    }

    public List<Record> peopleToInform() {
        var dbVisualizationQuery =
                "match (sick:Person)-[v1:VISITS]->(place:Place)<-[v2:VISITS]-(healthy:Person)\n" +
                        "with sick, healthy,\n" +
                        "    duration.inSeconds(,   \n" +
                        "    apoc.coll.min([v1.endtime, v2.endtime]), apoc.coll.max([v1.starttime, v2.starttime])) as chev, \n" +
                        "    duration({hours: 2}) as duration\n" +
                        "    where sick.healthstatus = \"Sick\" and healthy.healthstatus = \"Healthy\" and datetime() + chev >= datetime() + duration and v1.starttime >= sick.confirmedtime\n" +
                        "return sick.name, collect(distinct healthy.name) as peopleToInform";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
    }

    public List<Record> setHighRisk() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<Record> healthyCompanionsOf(String name) {
        /*
        The function 'apoc.path.subgraphNodes' was used instead of the notation saw in the slides because it has a better performance
        the minimum hop number can only be put at a maximum of 1
        This doesn't cause problem in this request because
        we need anyway a minimum of 2 hops to join two persons (person1->place<-person2).
         */
        var dbVisualizationQuery =
                "MATCH (person:Person)\n" +
                "WHERE  person.name = '"+name+"'\n" +
                "WITH person\n" +
                "CALL apoc.path.subgraphNodes(person , {\n" +
                "   minLevel : 1,\n" +
                "   maxLevel: 6,\n" +
                "   relationshipFilter : 'VISITS'\n" +
                "})\n" +
                "YIELD node\n" +
                "WHERE node:Person\n" +
                "WITH person, node as other\n" +
                "WHERE other.healthstatus = 'Healthy'\n" +
                "RETURN other.name as healthyName";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
    }

    public Record topSickSite() {
        var dbVisualizationQuery =
                "MATCH (sick:Person {healthstatus: 'Sick'})-[v:VISITS]->(pl:Place)\n" +
                "WHERE sick.confirmedtime < v.starttime\n" +
                "RETURN pl.type as placeType, size(collect(sick.id)) as nbOfSickVisits\n" +
                "ORDER BY nbOfSickVisits DESC\n" +
                "LIMIT 1";
        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list().get(0);
        }
    }

    public List<Record> sickFrom(List<String> names) {
        String personList = "";
        for (String name : names) {
            personList += "'"+name+"',";
        }
        personList = personList.substring(0, personList.length() - 1);
        var dbVisualizationQuery =
                "MATCH (p:Person {healthstatus: 'Sick'})\n" +
                "WHERE p.name IN ["+ personList +"]\n" +
                "RETURN p.name AS sickName";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
    }
}
