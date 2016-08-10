package aggregators;

public interface Aggregator {

  public String aggregateFields(String aggregationGrammar, String rawJson);

}
