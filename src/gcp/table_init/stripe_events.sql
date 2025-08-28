DROP TABLE IF EXISTS `thesis-25-456305.raw_layer.stripe_events`;

CREATE TABLE `thesis-25-456305.raw_layer.stripe_events` (
  id STRING,
  object STRING,
  api_version STRING,
  created INT64,
  type STRING,
  livemode BOOL,
  pending_webhooks INT64,
  request STRUCT<
    id STRING,
    idempotency_key STRING
  >,
  data STRUCT<
    object STRUCT<
      id STRING,
      object STRING,
      amount INT64,
      amount_captured INT64,
      amount_refunded INT64,
      application STRING,
      application_fee STRING,
      application_fee_amount INT64,
      balance_transaction STRING,
      billing_details STRUCT<
        address STRUCT<
          city STRING,
          country STRING,
          line1 STRING,
          line2 STRING,
          postal_code STRING,
          state STRING
        >,
        email STRING,
        name STRING,
        phone STRING,
        tax_id STRING
      >,
      calculated_statement_descriptor STRING,
      captured BOOL,
      created int64,
      currency STRING,
      customer STRING,
      description STRING,
      destination STRING,
      dispute STRING,
      disputed BOOL,
      failure_balance_transaction STRING,
      failure_code STRING,
      failure_message STRING,
      fraud_details JSON,
      livemode BOOL,
      metadata STRUCT<
        itemid STRING,
        lastname STRING,
        component STRING,
        userid STRING,
        firstname STRING,
        paymentarea STRING,
        username STRING
      >,
      on_behalf_of STRING,
      _order STRING,
      outcome STRUCT<
        advice_code STRING,
        network_advice_code STRING,
        network_decline_code STRING,
        network_status STRING,
        reason STRING,
        risk_level STRING,
        risk_score INT64,
        seller_message STRING,
        type STRING
      >,
      paid BOOL,
      payment_intent STRING,
      payment_method STRING,
      payment_method_details STRUCT<
        card STRUCT<
          amount_authorized INT64,
          authorization_code STRING,
          brand STRING,
          checks STRUCT<
            address_line1_check STRING,
            address_postal_code_check STRING,
            cvc_check STRING
          >,
          country STRING,
          exp_month INT64,
          exp_year INT64,
          extended_authorization STRUCT<status STRING>,
          fingerprint STRING,
          funding STRING,
          incremental_authorization STRUCT<status STRING>,
          installments STRING,
          last4 STRING,
          mandate STRING,
          multicapture STRUCT<status STRING>,
          network STRING,
          network_token STRUCT<used BOOL>,
          network_transaction_id STRING,
          overcapture STRUCT<
            maximum_amount_capturable INT64,
            status STRING
          >,
          regulated_status STRING,
          three_d_secure STRING,
          wallet STRING
        >,
        type STRING
      >,
      presentment_details STRUCT<
        presentment_amount INT64,
        presentment_currency STRING
      >,
      radar_options JSON,
      receipt_email STRING,
      receipt_number STRING,
      receipt_url STRING,
      refunded BOOL,
      review STRING,
      shipping STRING,
      source STRING,
      source_transfer STRING,
      statement_descriptor STRING,
      statement_descriptor_suffix STRING,
      status STRING,
      transfer_data STRING,
      transfer_group STRING
    >
  >
);