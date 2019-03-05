package uk.co.brayan.producer

// This object is used to store names of keys used in a JSON string.
object Transaction {
  val card_number = "card_number"
  val transaction_number = "transaction_number"
  val transaction_time = "transaction_time"
  val category = "category"
  val merchant = "merchant_name"
  val amount = "amount"
  val merchant_latitude = "merchant_latitude"
  val merchant_longitude = "merchant_longitude"
}