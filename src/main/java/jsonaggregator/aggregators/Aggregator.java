package jsonaggregator.aggregators;

import java.util.List;

import com.wizzardo.tools.json.JsonObject;

import jsonaggregator.grammar.Field;

public interface Aggregator {

  public List<Object> retrieveField(Field toAggregateField, JsonObject json);

}
