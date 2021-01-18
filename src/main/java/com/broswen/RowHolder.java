package com.broswen;

import java.util.ArrayList;
import java.util.List;

public class RowHolder {

  private List<Row> rows;

  RowHolder() {
    this.rows = new ArrayList<>();
  }

  public List<Row> getRows() {
    return this.rows;
  }

  public void addRow(Row row) {
    this.rows.add(row);
  }

  public int getLength() {
    return this.rows.size();
  }
  
}
