package com.broswen;

public class Row {
  public String id;
  public double amount;
  public String account;
  public String date;


  Row(String id, double amount, String account, String date) {
    this.id = id;
    this.amount = amount;
    this.account = account;
    this.date = date;
  }

  @Override
  public String toString() {
    return this.id + ", " + this.amount + ", " + this.account + ", " + this.date;
  }
  
}
