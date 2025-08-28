{{ config(
    partition_by={
        "field": "created",
        "data_type": "timestamp"
    }
) }}

SELECT
    id,
    created,
    data.object.amount,
    data.object.amount_captured,
    data.object.amount_refunded,

    -- Billing details
    data.object.billing_details.address.country AS billing_country,
    data.object.billing_details.email AS billing_email,
    data.object.billing_details.name AS billing_name,
    data.object.billing_details.phone AS billing_phone,

    -- Currency
    data.object.currency,
    data.object.paid,
    data.object.customer AS customer_id,

    -- Metadata
    data.object.metadata.itemid AS metadata_itemid,
    data.object.metadata.userid AS metadata_userid,
    data.object.metadata.username AS metadata_username,
    data.object.metadata.firstname AS metadata_firstname,
    data.object.metadata.lastname AS metadata_lastname,
    data.object.metadata.component AS metadata_component,
    data.object.metadata.paymentarea AS metadata_paymentarea,

    -- Card details
    data.object.payment_method_details.card.brand AS card_brand,
    data.object.payment_method_details.card.last4 AS card_last4,
    data.object.payment_method_details.card.country AS card_country,
    data.object.payment_method_details.card.funding AS card_funding,

    -- Receipt
    data.object.receipt_url
FROM `thesis-25-456305.raw_layer.stripe_events`