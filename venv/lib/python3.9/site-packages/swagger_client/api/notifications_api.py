# coding: utf-8

"""
    API Reference

      # Introduction  Welcome to the REST API reference for the Zuora Billing, Payments, and Central Platform!  To learn about the common use cases of Zuora REST APIs, check out the [REST API Tutorials](https://www.zuora.com/developer/rest-api/api-guides/overview/).  In addition to Zuora API Reference, we also provide API references for other Zuora products:    * [Revenue API Reference](https://www.zuora.com/developer/api-references/revenue/overview/)   * [Collections API Reference](https://www.zuora.com/developer/api-references/collections/overview/)      The Zuora REST API provides a broad set of operations and resources that:    * Enable Web Storefront integration from your website.   * Support self-service subscriber sign-ups and account management.   * Process revenue schedules through custom revenue rule models.   * Enable manipulation of most objects in the Zuora Billing Object Model.  Want to share your opinion on how our API works for you? <a href=\"https://community.zuora.com/t5/Developers/API-Feedback-Form/gpm-p/21399\" target=\"_blank\">Tell us how you feel </a>about using our API and what we can do to make it better.  Some of our older APIs are no longer recommended but still available, not affecting any existing integration. To find related API documentation, see [Older API Reference](https://www.zuora.com/developer/api-references/older-api/overview/).   ## Access to the API  If you have a Zuora tenant, you can access the Zuora REST API via one of the following endpoints:  | Tenant              | Base URL for REST Endpoints | |-------------------------|-------------------------| |US Cloud 1 Production | https://rest.na.zuora.com  | |US Cloud 1 API Sandbox |  https://rest.sandbox.na.zuora.com | |US Cloud 2 Production | https://rest.zuora.com | |US Cloud 2 API Sandbox | https://rest.apisandbox.zuora.com| |US Central Sandbox | https://rest.test.zuora.com |   |US Performance Test | https://rest.pt1.zuora.com | |US Production Copy | Submit a request at <a href=\"http://support.zuora.com/\" target=\"_blank\">Zuora Global Support</a> to enable the Zuora REST API in your tenant and obtain the base URL for REST endpoints. See [REST endpoint base URL of Production Copy (Service) Environment for existing and new customers](https://community.zuora.com/t5/API/REST-endpoint-base-URL-of-Production-Copy-Service-Environment/td-p/29611) for more information. | |EU Production | https://rest.eu.zuora.com | |EU API Sandbox | https://rest.sandbox.eu.zuora.com | |EU Central Sandbox | https://rest.test.eu.zuora.com |  The Production endpoint provides access to your live user data. Sandbox tenants are a good place to test code without affecting real-world data. If you would like Zuora to provision a Sandbox tenant for you, contact your Zuora representative for assistance.   If you do not have a Zuora tenant, go to <a href=\"https://www.zuora.com/resource/zuora-test-drive\" target=\"_blank\">https://www.zuora.com/resource/zuora-test-drive</a> and sign up for a Production Test Drive tenant. The tenant comes with seed data, including a sample product catalog.   # Error Handling  If a request to Zuora Billing REST API with an endpoint starting with `/v1` (except [Actions](https://www.zuora.com/developer/api-references/api/tag/Actions) and CRUD operations) fails, the response will contain an eight-digit error code with a corresponding error message to indicate the details of the error.  The following code snippet is a sample error response that contains an error code and message pair:  ```  {    \"success\": false,    \"processId\": \"CBCFED6580B4E076\",    \"reasons\":  [      {       \"code\": 53100320,       \"message\": \"'termType' value should be one of: TERMED, EVERGREEN\"      }     ]  } ``` The `success` field indicates whether the API request has succeeded. The `processId` field is a Zuora internal ID that you can provide to Zuora Global Support for troubleshooting purposes.  The `reasons` field contains the actual error code and message pair. The error code begins with `5` or `6` means that you encountered a certain issue that is specific to a REST API resource in Zuora Billing, Payments, and Central Platform. For example, `53100320` indicates that an invalid value is specified for the `termType` field of the `subscription` object.  The error code beginning with `9` usually indicates that an authentication-related issue occurred, and it can also indicate other unexpected errors depending on different cases. For example, `90000011` indicates that an invalid credential is provided in the request header.   When troubleshooting the error, you can divide the error code into two components: REST API resource code and error category code. See the following Zuora error code sample:  <a href=\"https://www.zuora.com/developer/images/ZuoraErrorCode.jpeg\" target=\"_blank\"><img src=\"https://www.zuora.com/developer/images/ZuoraErrorCode.jpeg\" alt=\"Zuora Error Code Sample\"></a>   **Note:** Zuora determines resource codes based on the request payload. Therefore, if GET and DELETE requests that do not contain payloads fail, you will get `500000` as the resource code, which indicates an unknown object and an unknown field.  The error category code of these requests is valid and follows the rules described in the [Error Category Codes](https://www.zuora.com/developer/api-references/api/overview/#section/Error-Handling/Error-Category-Codes) section.  In such case, you can refer to the returned error message to troubleshoot.   ## REST API Resource Codes  The 6-digit resource code indicates the REST API resource, typically a field of a Zuora object, on which the issue occurs. In the preceding example, `531003` refers to the `termType` field of the `subscription` object.   The value range for all REST API resource codes is from `500000` to `679999`. See <a href=\"https://knowledgecenter.zuora.com/Central_Platform/API/AA_REST_API/Resource_Codes\" target=\"_blank\">Resource Codes</a> in the Knowledge Center for a full list of resource codes.  ## Error Category Codes  The 2-digit error category code identifies the type of error, for example, resource not found or missing required field.   The following table describes all error categories and the corresponding resolution:  | Code    | Error category              | Description    | Resolution    | |:--------|:--------|:--------|:--------| | 10      | Permission or access denied | The request cannot be processed because a certain tenant or user permission is missing. | Check the missing tenant or user permission in the response message and contact <a href=\"https://support.zuora.com\" target=\"_blank\">Zuora Global Support</a> for enablement. | | 11      | Authentication failed       | Authentication fails due to invalid API authentication credentials. | Ensure that a valid API credential is specified. | | 20      | Invalid format or value     | The request cannot be processed due to an invalid field format or value. | Check the invalid field in the error message, and ensure that the format and value of all fields you passed in are valid. | | 21      | Unknown field in request    | The request cannot be processed because an unknown field exists in the request body. | Check the unknown field name in the response message, and ensure that you do not include any unknown field in the request body. | | 22      | Missing required field      | The request cannot be processed because a required field in the request body is missing. | Check the missing field name in the response message, and ensure that you include all required fields in the request body. | | 23      | Missing required parameter  | The request cannot be processed because a required query parameter is missing. | Check the missing parameter name in the response message, and ensure that you include the parameter in the query. | | 30      | Rule restriction            | The request cannot be processed due to the violation of a Zuora business rule. | Check the response message and ensure that the API request meets the specified business rules. | | 40      | Not found                   | The specified resource cannot be found. | Check the response message and ensure that the specified resource exists in your Zuora tenant. | | 45      | Unsupported request         | The requested endpoint does not support the specified HTTP method. | Check your request and ensure that the endpoint and method matches. | | 50      | Locking contention          | This request cannot be processed because the objects this request is trying to modify are being modified by another API request, UI operation, or batch job process. | <p>Resubmit the request first to have another try.</p> <p>If this error still occurs, contact <a href=\"https://support.zuora.com\" target=\"_blank\">Zuora Global Support</a> with the returned `Zuora-Request-Id` value in the response header for assistance.</p> | | 60      | Internal error              | The server encounters an internal error. | Contact <a href=\"https://support.zuora.com\" target=\"_blank\">Zuora Global Support</a> with the returned `Zuora-Request-Id` value in the response header for assistance. | | 61      | Temporary error             | A temporary error occurs during request processing, for example, a database communication error. | <p>Resubmit the request first to have another try.</p> <p>If this error still occurs, contact <a href=\"https://support.zuora.com\" target=\"_blank\">Zuora Global Support</a> with the returned `Zuora-Request-Id` value in the response header for assistance. </p>| | 70      | Request exceeded limit      | The total number of concurrent requests exceeds the limit allowed by the system. | <p>Resubmit the request after the number of seconds specified by the `Retry-After` value in the response header.</p> <p>Check [Concurrent request limits](https://www.zuora.com/developer/rest-api/general-concepts/rate-concurrency-limits/) for details about Zuora’s concurrent request limit policy.</p> | | 90      | Malformed request           | The request cannot be processed due to JSON syntax errors. | Check the syntax error in the JSON request body and ensure that the request is in the correct JSON format. | | 99      | Integration error           | The server encounters an error when communicating with an external system, for example, payment gateway, tax engine provider. | Check the response message and take action accordingly. |   # API Versions  The Zuora REST API are version controlled. Versioning ensures that Zuora REST API changes are backward compatible. Zuora uses a major and minor version nomenclature to manage changes. By specifying a version in a REST request, you can get expected responses regardless of future changes to the API.  ## Major Version  The major version number of the REST API appears in the REST URL. In this API reference, only the **v1** major version is available. For example, `POST https://rest.zuora.com/v1/subscriptions`.  ## Minor Version  Zuora uses minor versions for the REST API to control small changes. For example, a field in a REST method is deprecated and a new field is used to replace it.   Some fields in the REST methods are supported as of minor versions. If a field is not noted with a minor version, this field is available for all minor versions. If a field is noted with a minor version, this field is in version control. You must specify the supported minor version in the request header to process without an error.   If a field is in version control, it is either with a minimum minor version or a maximum minor version, or both of them. You can only use this field with the minor version between the minimum and the maximum minor versions. For example, the `invoiceCollect` field in the POST Subscription method is in version control and its maximum minor version is 189.0. You can only use this field with the minor version 189.0 or earlier.  If you specify a version number in the request header that is not supported, Zuora will use the minimum minor version of the REST API. In our REST API documentation, if a field or feature requires a minor version number, we note that in the field description.  You only need to specify the version number when you use the fields require a minor version. To specify the minor version, set the `zuora-version` parameter to the minor version number in the request header for the request call. For example, the `collect` field is in 196.0 minor version. If you want to use this field for the POST Subscription method, set the  `zuora-version` parameter to `196.0` in the request header. The `zuora-version` parameter is case sensitive.  For all the REST API fields, by default, if the minor version is not specified in the request header, Zuora will use the minimum minor version of the REST API to avoid breaking your integration.   ### Minor Version History  The supported minor versions are not serial. This section documents the changes made to each Zuora REST API minor version.  The following table lists the supported versions and the fields that have a Zuora REST API minor version.  | Fields         | Minor Version      | REST Methods    | Description | |:--------|:--------|:--------|:--------| | invoiceCollect | 189.0 and earlier  | [Create Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_Subscription \"Create Subscription\"); [Update Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_Subscription \"Update Subscription\"); [Renew Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_RenewSubscription \"Renew Subscription\"); [Cancel Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_CancelSubscription \"Cancel Subscription\"); [Suspend Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_SuspendSubscription \"Suspend Subscription\"); [Resume Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_ResumeSubscription \"Resume Subscription\"); [Create Account](https://www.zuora.com/developer/api-references/api/operation/POST_Account \"Create Account\")|Generates an invoice and collects a payment for a subscription. | | collect        | 196.0 and later    | [Create Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_Subscription \"Create Subscription\"); [Update Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_Subscription \"Update Subscription\"); [Renew Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_RenewSubscription \"Renew Subscription\"); [Cancel Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_CancelSubscription \"Cancel Subscription\"); [Suspend Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_SuspendSubscription \"Suspend Subscription\"); [Resume Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_ResumeSubscription \"Resume Subscription\"); [Create Account](https://www.zuora.com/developer/api-references/api/operation/POST_Account \"Create Account\")|Collects an automatic payment for a subscription. | | invoice | 196.0 and 207.0| [Create Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_Subscription \"Create Subscription\"); [Update Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_Subscription \"Update Subscription\"); [Renew Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_RenewSubscription \"Renew Subscription\"); [Cancel Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_CancelSubscription \"Cancel Subscription\"); [Suspend Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_SuspendSubscription \"Suspend Subscription\"); [Resume Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_ResumeSubscription \"Resume Subscription\"); [Create Account](https://www.zuora.com/developer/api-references/api/operation/POST_Account \"Create Account\")|Generates an invoice for a subscription. | | invoiceTargetDate | 206.0 and earlier  | [Preview Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_PreviewSubscription \"Preview Subscription\") |Date through which charges are calculated on the invoice, as `yyyy-mm-dd`. | | invoiceTargetDate | 207.0 and earlier  | [Create Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_Subscription \"Create Subscription\"); [Update Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_Subscription \"Update Subscription\"); [Renew Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_RenewSubscription \"Renew Subscription\"); [Cancel Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_CancelSubscription \"Cancel Subscription\"); [Suspend Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_SuspendSubscription \"Suspend Subscription\"); [Resume Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_ResumeSubscription \"Resume Subscription\"); [Create Account](https://www.zuora.com/developer/api-references/api/operation/POST_Account \"Create Account\")|Date through which charges are calculated on the invoice, as `yyyy-mm-dd`. | | targetDate | 207.0 and later | [Preview Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_PreviewSubscription \"Preview Subscription\") |Date through which charges are calculated on the invoice, as `yyyy-mm-dd`. | | targetDate | 211.0 and later | [Create Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_Subscription \"Create Subscription\"); [Update Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_Subscription \"Update Subscription\"); [Renew Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_RenewSubscription \"Renew Subscription\"); [Cancel Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_CancelSubscription \"Cancel Subscription\"); [Suspend Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_SuspendSubscription \"Suspend Subscription\"); [Resume Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_ResumeSubscription \"Resume Subscription\"); [Create Account](https://www.zuora.com/developer/api-references/api/operation/POST_Account \"Create Account\")|Date through which charges are calculated on the invoice, as `yyyy-mm-dd`. | | includeExisting DraftInvoiceItems | 206.0 and earlier| [Preview Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_PreviewSubscription \"Preview Subscription\"); [Update Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_Subscription \"Update Subscription\") | Specifies whether to include draft invoice items in subscription previews. Specify it to be `true` (default) to include draft invoice items in the preview result. Specify it to be `false` to excludes draft invoice items in the preview result. | | includeExisting DraftDocItems | 207.0 and later  | [Preview Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_PreviewSubscription \"Preview Subscription\"); [Update Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_Subscription \"Update Subscription\") | Specifies whether to include draft invoice items in subscription previews. Specify it to be `true` (default) to include draft invoice items in the preview result. Specify it to be `false` to excludes draft invoice items in the preview result. | | previewType | 206.0 and earlier| [Preview Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_PreviewSubscription \"Preview Subscription\"); [Update Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_Subscription \"Update Subscription\") | The type of preview you will receive. The possible values are `InvoiceItem`(default), `ChargeMetrics`, and `InvoiceItemChargeMetrics`. | | previewType | 207.0 and later  | [Preview Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_PreviewSubscription \"Preview Subscription\"); [Update Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_Subscription \"Update Subscription\") | The type of preview you will receive. The possible values are `LegalDoc`(default), `ChargeMetrics`, and `LegalDocChargeMetrics`. | | runBilling  | 211.0 and later  | [Create Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_Subscription \"Create Subscription\"); [Update Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_Subscription \"Update Subscription\"); [Renew Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_RenewSubscription \"Renew Subscription\"); [Cancel Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_CancelSubscription \"Cancel Subscription\"); [Suspend Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_SuspendSubscription \"Suspend Subscription\"); [Resume Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_ResumeSubscription \"Resume Subscription\"); [Create Account](https://www.zuora.com/developer/api-references/api/operation/POST_Account \"Create Account\")|Generates an invoice or credit memo for a subscription. **Note:** Credit memos are only available if you have the Invoice Settlement feature enabled. | | invoiceDate | 214.0 and earlier  | [Invoice and Collect](https://www.zuora.com/developer/api-references/api/operation/POST_TransactionInvoicePayment \"Invoice and Collect\") |Date that should appear on the invoice being generated, as `yyyy-mm-dd`. | | invoiceTargetDate | 214.0 and earlier  | [Invoice and Collect](https://www.zuora.com/developer/api-references/api/operation/POST_TransactionInvoicePayment \"Invoice and Collect\") |Date through which to calculate charges on this account if an invoice is generated, as `yyyy-mm-dd`. | | documentDate | 215.0 and later | [Invoice and Collect](https://www.zuora.com/developer/api-references/api/operation/POST_TransactionInvoicePayment \"Invoice and Collect\") |Date that should appear on the invoice and credit memo being generated, as `yyyy-mm-dd`. | | targetDate | 215.0 and later | [Invoice and Collect](https://www.zuora.com/developer/api-references/api/operation/POST_TransactionInvoicePayment \"Invoice and Collect\") |Date through which to calculate charges on this account if an invoice or a credit memo is generated, as `yyyy-mm-dd`. | | memoItemAmount | 223.0 and earlier | [Create credit memo from charge](https://www.zuora.com/developer/api-references/api/operation/POST_CreditMemoFromPrpc \"Create credit memo from charge\"); [Create debit memo from charge](https://www.zuora.com/developer/api-references/api/operation/POST_DebitMemoFromPrpc \"Create debit memo from charge\") | Amount of the memo item. | | amount | 224.0 and later | [Create credit memo from charge](https://www.zuora.com/developer/api-references/api/operation/POST_CreditMemoFromPrpc \"Create credit memo from charge\"); [Create debit memo from charge](https://www.zuora.com/developer/api-references/api/operation/POST_DebitMemoFromPrpc \"Create debit memo from charge\") | Amount of the memo item. | | subscriptionNumbers | 222.4 and earlier | [Create order](https://www.zuora.com/developer/api-references/api/operation/POST_Order \"Create order\") | Container for the subscription numbers of the subscriptions in an order. | | subscriptions | 223.0 and later | [Create order](https://www.zuora.com/developer/api-references/api/operation/POST_Order \"Create order\") | Container for the subscription numbers and statuses in an order. | | creditTaxItems | 238.0 and earlier | [Get credit memo items](https://www.zuora.com/developer/api-references/api/operation/GET_CreditMemoItems \"Get credit memo items\"); [Get credit memo item](https://www.zuora.com/developer/api-references/api/operation/GET_CreditMemoItem \"Get credit memo item\") | Container for the taxation items of the credit memo item. | | taxItems | 238.0 and earlier | [Get debit memo items](https://www.zuora.com/developer/api-references/api/operation/GET_DebitMemoItems \"Get debit memo items\"); [Get debit memo item](https://www.zuora.com/developer/api-references/api/operation/GET_DebitMemoItem \"Get debit memo item\") | Container for the taxation items of the debit memo item. | | taxationItems | 239.0 and later | [Get credit memo items](https://www.zuora.com/developer/api-references/api/operation/GET_CreditMemoItems \"Get credit memo items\"); [Get credit memo item](https://www.zuora.com/developer/api-references/api/operation/GET_CreditMemoItem \"Get credit memo item\"); [Get debit memo items](https://www.zuora.com/developer/api-references/api/operation/GET_DebitMemoItems \"Get debit memo items\"); [Get debit memo item](https://www.zuora.com/developer/api-references/api/operation/GET_DebitMemoItem \"Get debit memo item\") | Container for the taxation items of the memo item. | | chargeId | 256.0 and earlier | [Create credit memo from charge](https://www.zuora.com/developer/api-references/api/operation/POST_CreditMemoFromPrpc \"Create credit memo from charge\"); [Create debit memo from charge](https://www.zuora.com/developer/api-references/api/operation/POST_DebitMemoFromPrpc \"Create debit memo from charge\") | ID of the product rate plan charge that the memo is created from. | | productRatePlanChargeId | 257.0 and later | [Create credit memo from charge](https://www.zuora.com/developer/api-references/api/operation/POST_CreditMemoFromPrpc \"Create credit memo from charge\"); [Create debit memo from charge](https://www.zuora.com/developer/api-references/api/operation/POST_DebitMemoFromPrpc \"Create debit memo from charge\") | ID of the product rate plan charge that the memo is created from. | | comment | 256.0 and earlier | [Create credit memo from charge](https://www.zuora.com/developer/api-references/api/operation/POST_CreditMemoFromPrpc \"Create credit memo from charge\"); [Create debit memo from charge](https://www.zuora.com/developer/api-references/api/operation/POST_DebitMemoFromPrpc \"Create debit memo from charge\"); [Create credit memo from invoice](https://www.zuora.com/developer/api-references/api/operation/POST_CreditMemoFromInvoice \"Create credit memo from invoice\"); [Create debit memo from invoice](https://www.zuora.com/developer/api-references/api/operation/POST_DebitMemoFromInvoice \"Create debit memo from invoice\"); [Get credit memo items](https://www.zuora.com/developer/api-references/api/operation/GET_CreditMemoItems \"Get credit memo items\"); [Get credit memo item](https://www.zuora.com/developer/api-references/api/operation/GET_CreditMemoItem \"Get credit memo item\"); [Get debit memo items](https://www.zuora.com/developer/api-references/api/operation/GET_DebitMemoItems \"Get debit memo items\"); [Get debit memo item](https://www.zuora.com/developer/api-references/api/operation/GET_DebitMemoItem \"Get debit memo item\") | Comments about the product rate plan charge, invoice item, or memo item. | | description | 257.0 and later | [Create credit memo from charge](https://www.zuora.com/developer/api-references/api/operation/POST_CreditMemoFromPrpc \"Create credit memo from charge\"); [Create debit memo from charge](https://www.zuora.com/developer/api-references/api/operation/POST_DebitMemoFromPrpc \"Create debit memo from charge\"); [Create credit memo from invoice](https://www.zuora.com/developer/api-references/api/operation/POST_CreditMemoFromInvoice \"Create credit memo from invoice\"); [Create debit memo from invoice](https://www.zuora.com/developer/api-references/api/operation/POST_DebitMemoFromInvoice \"Create debit memo from invoice\"); [Get credit memo items](https://www.zuora.com/developer/api-references/api/operation/GET_CreditMemoItems \"Get credit memo items\"); [Get credit memo item](https://www.zuora.com/developer/api-references/api/operation/GET_CreditMemoItem \"Get credit memo item\"); [Get debit memo items](https://www.zuora.com/developer/api-references/api/operation/GET_DebitMemoItems \"Get debit memo items\"); [Get debit memo item](https://www.zuora.com/developer/api-references/api/operation/GET_DebitMemoItem \"Get debit memo item\") | Description of the the product rate plan charge, invoice item, or memo item. | | taxationItems | 309.0 and later | [Preview an order](https://www.zuora.com/developer/api-references/api/operation/POST_PreviewOrder \"Preview an order\") | List of taxation items for an invoice item or a credit memo item. | | batch | 309.0 and earlier | [Create a billing preview run](https://www.zuora.com/developer/api-references/api/operation/POST_BillingPreviewRun \"Create a billing preview run\") | The customer batches to include in the billing preview run. |       | batches | 314.0 and later | [Create a billing preview run](https://www.zuora.com/developer/api-references/api/operation/POST_BillingPreviewRun \"Create a billing preview run\") | The customer batches to include in the billing preview run. | | taxationItems | 315.0 and later | [Preview a subscription](https://www.zuora.com/developer/api-references/api/operation/POST_PreviewSubscription \"Preview a subscription\"); [Update a subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_Subscription \"Update a subscription\")| List of taxation items for an invoice item or a credit memo item. | | billingDocument | 330.0 and later | [Create a payment schedule](https://www.zuora.com/developer/api-references/api/operation/POST_PaymentSchedule \"Create a payment schedule\"); [Create multiple payment schedules at once](https://www.zuora.com/developer/api-references/api/operation/POST_PaymentSchedules \"Create multiple payment schedules at once\")| The billing document with which the payment schedule item is associated. | | paymentId | 336.0 and earlier | [Add payment schedule items to a custom payment schedule](https://www.zuora.com/developer/api-references/api/operation/POST_AddItemsToCustomPaymentSchedule/ \"Add payment schedule items to a custom payment schedule\"); [Update a payment schedule](https://www.zuora.com/developer/api-references/api/operation/PUT_PaymentSchedule/ \"Update a payment schedule\"); [Update a payment schedule item](https://www.zuora.com/developer/api-references/api/operation/PUT_PaymentScheduleItem/ \"Update a payment schedule item\"); [Preview the result of payment schedule update](https://www.zuora.com/developer/api-references/api/operation/PUT_PaymentScheduleUpdatePreview/ \"Preview the result of payment schedule update\"); [Retrieve a payment schedule](https://www.zuora.com/developer/api-references/api/operation/GET_PaymentSchedule/ \"Retrieve a payment schedule\"); [Retrieve a payment schedule item](https://www.zuora.com/developer/api-references/api/operation/GET_PaymentScheduleItem/ \"Retrieve a payment schedule item\"); [List payment schedules by customer account](https://www.zuora.com/developer/api-references/api/operation/GET_PaymentSchedules/ \"List payment schedules by customer account\"); [Cancel a payment schedule](https://www.zuora.com/developer/api-references/api/operation/PUT_CancelPaymentSchedule/ \"Cancel a payment schedule\"); [Cancel a payment schedule item](https://www.zuora.com/developer/api-references/api/operation/PUT_CancelPaymentScheduleItem/ \"Cancel a payment schedule item\");[Skip a payment schedule item](https://www.zuora.com/developer/api-references/api/operation/PUT_SkipPaymentScheduleItem/ \"Skip a payment schedule item\");[Retry failed payment schedule items](https://www.zuora.com/developer/api-references/api/operation/POST_RetryPaymentScheduleItem/ \"Retry failed payment schedule items\") | ID of the payment to be linked to the payment schedule item. | | paymentOption | 337.0 and later | [Create a payment schedule](https://www.zuora.com/developer/api-references/api/operation/POST_PaymentSchedule/ \"Create a payment schedule\"); [Create multiple payment schedules at once](https://www.zuora.com/developer/api-references/api/operation/POST_PaymentSchedules/ \"Create multiple payment schedules at once\"); [Create a payment](https://www.zuora.com/developer/api-references/api/operation/POST_CreatePayment/ \"Create a payment\"); [Add payment schedule items to a custom payment schedule](https://www.zuora.com/developer/api-references/api/operation/POST_AddItemsToCustomPaymentSchedule/ \"Add payment schedule items to a custom payment schedule\"); [Update a payment schedule](https://www.zuora.com/developer/api-references/api/operation/PUT_PaymentSchedule/ \"Update a payment schedule\"); [Update a payment schedule item](https://www.zuora.com/developer/api-references/api/operation/PUT_PaymentScheduleItem/ \"Update a payment schedule item\"); [Preview the result of payment schedule update](https://www.zuora.com/developer/api-references/api/operation/PUT_PaymentScheduleUpdatePreview/ \"Preview the result of payment schedule update\"); [Retrieve a payment schedule](https://www.zuora.com/developer/api-references/api/operation/GET_PaymentSchedule/ \"Retrieve a payment schedule\"); [Retrieve a payment schedule item](https://www.zuora.com/developer/api-references/api/operation/GET_PaymentScheduleItem/ \"Retrieve a payment schedule item\"); [List payment schedules by customer account](https://www.zuora.com/developer/api-references/api/operation/GET_PaymentSchedules/ \"List payment schedules by customer account\"); [Cancel a payment schedule](https://www.zuora.com/developer/api-references/api/operation/PUT_CancelPaymentSchedule/ \"Cancel a payment schedule\"); [Cancel a payment schedule item](https://www.zuora.com/developer/api-references/api/operation/PUT_CancelPaymentScheduleItem/ \"Cancel a payment schedule item\"); [Skip a payment schedule item](https://www.zuora.com/developer/api-references/api/operation/PUT_SkipPaymentScheduleItem/ \"Skip a payment schedule item\"); [Retry failed payment schedule items](https://www.zuora.com/developer/api-references/api/operation/POST_RetryPaymentScheduleItem/ \"Retry failed payment schedule items\"); [List payments](https://www.zuora.com/developer/api-references/api/operation/GET_RetrieveAllPayments/ \"List payments\") | Array of transactional level rules for processing payments. |    #### Version 207.0 and Later  The response structure of the [Preview Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_PreviewSubscription) and [Update Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_Subscription \"Update Subscription\") methods are changed. The following invoice related response fields are moved to the invoice container:    * amount   * amountWithoutTax   * taxAmount   * invoiceItems   * targetDate   * chargeMetrics   # API Names for Zuora Objects  For information about the Zuora business object model, see [Zuora Business Object Model](https://www.zuora.com/developer/rest-api/general-concepts/object-model/).  You can use the [Describe](https://www.zuora.com/developer/api-references/api/operation/GET_Describe) operation to list the fields of each Zuora object that is available in your tenant. When you call the operation, you must specify the API name of the Zuora object.  The following table provides the API name of each Zuora object:  | Object                                        | API Name                                   | |-----------------------------------------------|--------------------------------------------| | Account                                       | `Account`                                  | | Accounting Code                               | `AccountingCode`                           | | Accounting Period                             | `AccountingPeriod`                         | | Amendment                                     | `Amendment`                                | | Application Group                             | `ApplicationGroup`                         | | Billing Run                                   | <p>`BillingRun` - API name used  in the [Describe](https://www.zuora.com/developer/api-references/api/operation/GET_Describe) operation, Export ZOQL queries, and Data Query.</p> <p>`BillRun` - API name used in the [Actions](https://www.zuora.com/developer/api-references/api/tag/Actions). See the CRUD oprations of [Bill Run](https://www.zuora.com/developer/api-references/api/tag/Bill-Run) for more information about the `BillRun` object. `BillingRun` and `BillRun` have different fields. |                      | Configuration Templates                       | `ConfigurationTemplates`                  | | Contact                                       | `Contact`                                  | | Contact Snapshot                              | `ContactSnapshot`                          | | Credit Balance Adjustment                     | `CreditBalanceAdjustment`                  | | Credit Memo                                   | `CreditMemo`                               | | Credit Memo Application                       | `CreditMemoApplication`                    | | Credit Memo Application Item                  | `CreditMemoApplicationItem`                | | Credit Memo Item                              | `CreditMemoItem`                           | | Credit Memo Part                              | `CreditMemoPart`                           | | Credit Memo Part Item                         | `CreditMemoPartItem`                       | | Credit Taxation Item                          | `CreditTaxationItem`                       | | Custom Exchange Rate                          | `FXCustomRate`                             | | Debit Memo                                    | `DebitMemo`                                | | Debit Memo Item                               | `DebitMemoItem`                            | | Debit Taxation Item                           | `DebitTaxationItem`                        | | Discount Applied Metrics                      | `DiscountAppliedMetrics`                   | | Entity                                        | `Tenant`                                   | | Fulfillment                                   | `Fulfillment`                              | | Feature                                       | `Feature`                                  | | Gateway Reconciliation Event                  | `PaymentGatewayReconciliationEventLog`     | | Gateway Reconciliation Job                    | `PaymentReconciliationJob`                 | | Gateway Reconciliation Log                    | `PaymentReconciliationLog`                 | | Invoice                                       | `Invoice`                                  | | Invoice Adjustment                            | `InvoiceAdjustment`                        | | Invoice Item                                  | `InvoiceItem`                              | | Invoice Item Adjustment                       | `InvoiceItemAdjustment`                    | | Invoice Payment                               | `InvoicePayment`                           | | Invoice Schedule                              | `InvoiceSchedule`                          | | Journal Entry                                 | `JournalEntry`                             | | Journal Entry Item                            | `JournalEntryItem`                         | | Journal Run                                   | `JournalRun`                               | | Notification History - Callout                | `CalloutHistory`                           | | Notification History - Email                  | `EmailHistory`                             | | Offer                                         | `Offer`                             | | Order                                         | `Order`                                    | | Order Action                                  | `OrderAction`                              | | Order ELP                                     | `OrderElp`                                 | | Order Line Items                              | `OrderLineItems`                           |     | Order Item                                    | `OrderItem`                                | | Order MRR                                     | `OrderMrr`                                 | | Order Quantity                                | `OrderQuantity`                            | | Order TCB                                     | `OrderTcb`                                 | | Order TCV                                     | `OrderTcv`                                 | | Payment                                       | `Payment`                                  | | Payment Application                           | `PaymentApplication`                       | | Payment Application Item                      | `PaymentApplicationItem`                   | | Payment Method                                | `PaymentMethod`                            | | Payment Method Snapshot                       | `PaymentMethodSnapshot`                    | | Payment Method Transaction Log                | `PaymentMethodTransactionLog`              | | Payment Method Update                        | `UpdaterDetail`                            | | Payment Part                                  | `PaymentPart`                              | | Payment Part Item                             | `PaymentPartItem`                          | | Payment Run                                   | `PaymentRun`                               | | Payment Transaction Log                       | `PaymentTransactionLog`                    | | Price Book Item                               | `PriceBookItem`                            | | Processed Usage                               | `ProcessedUsage`                           | | Product                                       | `Product`                                  | | Product Feature                               | `ProductFeature`                           | | Product Rate Plan                             | `ProductRatePlan`                          | | Product Rate Plan Charge                      | `ProductRatePlanCharge`                    | | Product Rate Plan Charge Tier                 | `ProductRatePlanChargeTier`                | | Rate Plan                                     | `RatePlan`                                 | | Rate Plan Charge                              | `RatePlanCharge`                           | | Rate Plan Charge Tier                         | `RatePlanChargeTier`                       | | Refund                                        | `Refund`                                   | | Refund Application                            | `RefundApplication`                        | | Refund Application Item                       | `RefundApplicationItem`                    | | Refund Invoice Payment                        | `RefundInvoicePayment`                     | | Refund Part                                   | `RefundPart`                               | | Refund Part Item                              | `RefundPartItem`                           | | Refund Transaction Log                        | `RefundTransactionLog`                     | | Revenue Charge Summary                        | `RevenueChargeSummary`                     | | Revenue Charge Summary Item                   | `RevenueChargeSummaryItem`                 | | Revenue Event                                 | `RevenueEvent`                             | | Revenue Event Credit Memo Item                | `RevenueEventCreditMemoItem`               | | Revenue Event Debit Memo Item                 | `RevenueEventDebitMemoItem`                | | Revenue Event Invoice Item                    | `RevenueEventInvoiceItem`                  | | Revenue Event Invoice Item Adjustment         | `RevenueEventInvoiceItemAdjustment`        | | Revenue Event Item                            | `RevenueEventItem`                         | | Revenue Event Item Credit Memo Item           | `RevenueEventItemCreditMemoItem`           | | Revenue Event Item Debit Memo Item            | `RevenueEventItemDebitMemoItem`            | | Revenue Event Item Invoice Item               | `RevenueEventItemInvoiceItem`              | | Revenue Event Item Invoice Item Adjustment    | `RevenueEventItemInvoiceItemAdjustment`    | | Revenue Event Type                            | `RevenueEventType`                         | | Revenue Schedule                              | `RevenueSchedule`                          | | Revenue Schedule Credit Memo Item             | `RevenueScheduleCreditMemoItem`            | | Revenue Schedule Debit Memo Item              | `RevenueScheduleDebitMemoItem`             | | Revenue Schedule Invoice Item                 | `RevenueScheduleInvoiceItem`               | | Revenue Schedule Invoice Item Adjustment      | `RevenueScheduleInvoiceItemAdjustment`     | | Revenue Schedule Item                         | `RevenueScheduleItem`                      | | Revenue Schedule Item Credit Memo Item        | `RevenueScheduleItemCreditMemoItem`        | | Revenue Schedule Item Debit Memo Item         | `RevenueScheduleItemDebitMemoItem`         | | Revenue Schedule Item Invoice Item            | `RevenueScheduleItemInvoiceItem`           | | Revenue Schedule Item Invoice Item Adjustment | `RevenueScheduleItemInvoiceItemAdjustment` | | Subscription                                  | `Subscription`                             | | Subscription Product Feature                  | `SubscriptionProductFeature`               | | Taxable Item Snapshot                         | `TaxableItemSnapshot`                      | | Taxation Item                                 | `TaxationItem`                             | | Updater Batch                                 | `UpdaterBatch`                             | | Usage                                         | `Usage`                                    |   # noqa: E501

    OpenAPI spec version: 2023-07-24
    Contact: docs@zuora.com
    Generated by: https://github.com/swagger-api/swagger-codegen.git
"""


from __future__ import absolute_import

import re  # noqa: F401

# python 2 and python 3 compatibility library
import six

from swagger_client.api_client import ApiClient


class NotificationsApi(object):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    Ref: https://github.com/swagger-api/swagger-codegen
    """

    def __init__(self, api_client=None):
        if api_client is None:
            api_client = ApiClient()
        self.api_client = api_client

    def d_elete_delete_email_template(self, authorization, id, **kwargs):  # noqa: E501
        """Delete an email template  # noqa: E501

        Deletes an email template. This operation supports deleting an email template for all event types.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.d_elete_delete_email_template(authorization, id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param str id: The ID of the email template to be deleted. (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: None
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.d_elete_delete_email_template_with_http_info(authorization, id, **kwargs)  # noqa: E501
        else:
            (data) = self.d_elete_delete_email_template_with_http_info(authorization, id, **kwargs)  # noqa: E501
            return data

    def d_elete_delete_email_template_with_http_info(self, authorization, id, **kwargs):  # noqa: E501
        """Delete an email template  # noqa: E501

        Deletes an email template. This operation supports deleting an email template for all event types.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.d_elete_delete_email_template_with_http_info(authorization, id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param str id: The ID of the email template to be deleted. (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: None
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['authorization', 'id', 'accept_encoding', 'content_encoding', 'zuora_track_id', 'zuora_entity_ids']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method d_elete_delete_email_template" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'authorization' is set
        if self.api_client.client_side_validation and ('authorization' not in params or
                                                       params['authorization'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `authorization` when calling `d_elete_delete_email_template`")  # noqa: E501
        # verify the required parameter 'id' is set
        if self.api_client.client_side_validation and ('id' not in params or
                                                       params['id'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `id` when calling `d_elete_delete_email_template`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `d_elete_delete_email_template`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'id' in params:
            path_params['id'] = params['id']  # noqa: E501

        query_params = []

        header_params = {}
        if 'accept_encoding' in params:
            header_params['Accept-Encoding'] = params['accept_encoding']  # noqa: E501
        if 'content_encoding' in params:
            header_params['Content-Encoding'] = params['content_encoding']  # noqa: E501
        if 'authorization' in params:
            header_params['Authorization'] = params['authorization']  # noqa: E501
        if 'zuora_track_id' in params:
            header_params['Zuora-Track-Id'] = params['zuora_track_id']  # noqa: E501
        if 'zuora_entity_ids' in params:
            header_params['Zuora-Entity-Ids'] = params['zuora_entity_ids']  # noqa: E501

        form_params = []
        local_var_files = {}

        body_params = None
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/notifications/email-templates/{id}', 'DELETE',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type=None,  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def d_elete_delete_notification_definition(self, authorization, id, **kwargs):  # noqa: E501
        """Delete a notification definition  # noqa: E501

        Deletes a notification definition.  **Note**: This operation is only applicable to notifications for custom events and custom scheduled events. You can use the Settings API to delete notifications for standard events. For more information, see [Delete a notification definition](https://knowledgecenter.zuora.com/Zuora_Central_Platform/API/BB_C_Settings_API/Settings_API_tutorials/Delete_a_notification_definition).   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.d_elete_delete_notification_definition(authorization, id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param str id: The ID of the notification definition to be deleted. (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: None
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.d_elete_delete_notification_definition_with_http_info(authorization, id, **kwargs)  # noqa: E501
        else:
            (data) = self.d_elete_delete_notification_definition_with_http_info(authorization, id, **kwargs)  # noqa: E501
            return data

    def d_elete_delete_notification_definition_with_http_info(self, authorization, id, **kwargs):  # noqa: E501
        """Delete a notification definition  # noqa: E501

        Deletes a notification definition.  **Note**: This operation is only applicable to notifications for custom events and custom scheduled events. You can use the Settings API to delete notifications for standard events. For more information, see [Delete a notification definition](https://knowledgecenter.zuora.com/Zuora_Central_Platform/API/BB_C_Settings_API/Settings_API_tutorials/Delete_a_notification_definition).   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.d_elete_delete_notification_definition_with_http_info(authorization, id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param str id: The ID of the notification definition to be deleted. (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: None
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['authorization', 'id', 'accept_encoding', 'content_encoding', 'zuora_track_id', 'zuora_entity_ids']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method d_elete_delete_notification_definition" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'authorization' is set
        if self.api_client.client_side_validation and ('authorization' not in params or
                                                       params['authorization'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `authorization` when calling `d_elete_delete_notification_definition`")  # noqa: E501
        # verify the required parameter 'id' is set
        if self.api_client.client_side_validation and ('id' not in params or
                                                       params['id'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `id` when calling `d_elete_delete_notification_definition`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `d_elete_delete_notification_definition`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'id' in params:
            path_params['id'] = params['id']  # noqa: E501

        query_params = []

        header_params = {}
        if 'accept_encoding' in params:
            header_params['Accept-Encoding'] = params['accept_encoding']  # noqa: E501
        if 'content_encoding' in params:
            header_params['Content-Encoding'] = params['content_encoding']  # noqa: E501
        if 'authorization' in params:
            header_params['Authorization'] = params['authorization']  # noqa: E501
        if 'zuora_track_id' in params:
            header_params['Zuora-Track-Id'] = params['zuora_track_id']  # noqa: E501
        if 'zuora_entity_ids' in params:
            header_params['Zuora-Entity-Ids'] = params['zuora_entity_ids']  # noqa: E501

        form_params = []
        local_var_files = {}

        body_params = None
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/notifications/notification-definitions/{id}', 'DELETE',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type=None,  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def d_elete_delete_notification_history_for_account(self, authorization, account_id, **kwargs):  # noqa: E501
        """Delete notification histories for an account  # noqa: E501

        Delete all notification histories for the given account. All email and callout notifications for this account will be deleted upon successful operation.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.d_elete_delete_notification_history_for_account(authorization, account_id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param str account_id: The ID of the account whose notification histories are to be deleted. (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: NotificationsHistoryDeletionTaskResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.d_elete_delete_notification_history_for_account_with_http_info(authorization, account_id, **kwargs)  # noqa: E501
        else:
            (data) = self.d_elete_delete_notification_history_for_account_with_http_info(authorization, account_id, **kwargs)  # noqa: E501
            return data

    def d_elete_delete_notification_history_for_account_with_http_info(self, authorization, account_id, **kwargs):  # noqa: E501
        """Delete notification histories for an account  # noqa: E501

        Delete all notification histories for the given account. All email and callout notifications for this account will be deleted upon successful operation.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.d_elete_delete_notification_history_for_account_with_http_info(authorization, account_id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param str account_id: The ID of the account whose notification histories are to be deleted. (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: NotificationsHistoryDeletionTaskResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['authorization', 'account_id', 'accept_encoding', 'content_encoding', 'zuora_track_id', 'zuora_entity_ids']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method d_elete_delete_notification_history_for_account" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'authorization' is set
        if self.api_client.client_side_validation and ('authorization' not in params or
                                                       params['authorization'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `authorization` when calling `d_elete_delete_notification_history_for_account`")  # noqa: E501
        # verify the required parameter 'account_id' is set
        if self.api_client.client_side_validation and ('account_id' not in params or
                                                       params['account_id'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `account_id` when calling `d_elete_delete_notification_history_for_account`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `d_elete_delete_notification_history_for_account`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}

        query_params = []
        if 'account_id' in params:
            query_params.append(('accountId', params['account_id']))  # noqa: E501

        header_params = {}
        if 'accept_encoding' in params:
            header_params['Accept-Encoding'] = params['accept_encoding']  # noqa: E501
        if 'content_encoding' in params:
            header_params['Content-Encoding'] = params['content_encoding']  # noqa: E501
        if 'authorization' in params:
            header_params['Authorization'] = params['authorization']  # noqa: E501
        if 'zuora_track_id' in params:
            header_params['Zuora-Track-Id'] = params['zuora_track_id']  # noqa: E501
        if 'zuora_entity_ids' in params:
            header_params['Zuora-Entity-Ids'] = params['zuora_entity_ids']  # noqa: E501

        form_params = []
        local_var_files = {}

        body_params = None
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/notifications/history', 'DELETE',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='NotificationsHistoryDeletionTaskResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def g_et_callout_history(self, **kwargs):  # noqa: E501
        """List callout notification histories  # noqa: E501

        Describes how to get a notification history for callouts.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_callout_history(async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param int page: The index number of the page that you want to retrieve. This parameter is dependent on `pageSize`. You must set `pageSize` before specifying `page`. For example, if you set `pageSize` to `20` and `page` to `2`, the 21st to 40th records are returned in the response. 
        :param int page_size: The number of records returned per page in the response. 
        :param datetime end_time: The final date and time of records to be returned. Defaults to now. Use format yyyy-MM-ddTHH:mm:ss.
        :param datetime start_time: The initial date and time of records to be returned. Defaults to (end time - 1 day). Use format yyyy-MM-ddTHH:mm:ss.
        :param str object_id: The ID of an object that triggered a callout notification.  
        :param bool failed_only: If `true`, only return failed records. If `false`, return all records in the given date range. The default value is `true`. 
        :param str event_category: Category of records to be returned by event category.  The following formats are supported: * `{Event Type Namespace}:{Event Type Name}` if the Custom Events feature is enabled in your tenant. For example: `user.notification:NewSubscriptionCreated`. * Numeric code of the event category if the Custom Events feature is not enabled in your tenant. For example, `1210`. See [Event Category Code](https://knowledgecenter.zuora.com/DC_Developers/AA_REST_API/Event_Categories_for_Notification_Histories) for more information. 
        :param bool include_response_content: 
        :return: GETCalloutHistoryVOsType
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.g_et_callout_history_with_http_info(**kwargs)  # noqa: E501
        else:
            (data) = self.g_et_callout_history_with_http_info(**kwargs)  # noqa: E501
            return data

    def g_et_callout_history_with_http_info(self, **kwargs):  # noqa: E501
        """List callout notification histories  # noqa: E501

        Describes how to get a notification history for callouts.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_callout_history_with_http_info(async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param int page: The index number of the page that you want to retrieve. This parameter is dependent on `pageSize`. You must set `pageSize` before specifying `page`. For example, if you set `pageSize` to `20` and `page` to `2`, the 21st to 40th records are returned in the response. 
        :param int page_size: The number of records returned per page in the response. 
        :param datetime end_time: The final date and time of records to be returned. Defaults to now. Use format yyyy-MM-ddTHH:mm:ss.
        :param datetime start_time: The initial date and time of records to be returned. Defaults to (end time - 1 day). Use format yyyy-MM-ddTHH:mm:ss.
        :param str object_id: The ID of an object that triggered a callout notification.  
        :param bool failed_only: If `true`, only return failed records. If `false`, return all records in the given date range. The default value is `true`. 
        :param str event_category: Category of records to be returned by event category.  The following formats are supported: * `{Event Type Namespace}:{Event Type Name}` if the Custom Events feature is enabled in your tenant. For example: `user.notification:NewSubscriptionCreated`. * Numeric code of the event category if the Custom Events feature is not enabled in your tenant. For example, `1210`. See [Event Category Code](https://knowledgecenter.zuora.com/DC_Developers/AA_REST_API/Event_Categories_for_Notification_Histories) for more information. 
        :param bool include_response_content: 
        :return: GETCalloutHistoryVOsType
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids', 'page', 'page_size', 'end_time', 'start_time', 'object_id', 'failed_only', 'event_category', 'include_response_content']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method g_et_callout_history" % key
                )
            params[key] = val
        del params['kwargs']

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `g_et_callout_history`, length must be less than or equal to `64`")  # noqa: E501
        if self.api_client.client_side_validation and ('page' in params and params['page'] < 1):  # noqa: E501
            raise ValueError("Invalid value for parameter `page` when calling `g_et_callout_history`, must be a value greater than or equal to `1`")  # noqa: E501
        if self.api_client.client_side_validation and ('page_size' in params and params['page_size'] > 40):  # noqa: E501
            raise ValueError("Invalid value for parameter `page_size` when calling `g_et_callout_history`, must be a value less than or equal to `40`")  # noqa: E501
        collection_formats = {}

        path_params = {}

        query_params = []
        if 'page' in params:
            query_params.append(('page', params['page']))  # noqa: E501
        if 'page_size' in params:
            query_params.append(('pageSize', params['page_size']))  # noqa: E501
        if 'end_time' in params:
            query_params.append(('endTime', params['end_time']))  # noqa: E501
        if 'start_time' in params:
            query_params.append(('startTime', params['start_time']))  # noqa: E501
        if 'object_id' in params:
            query_params.append(('objectId', params['object_id']))  # noqa: E501
        if 'failed_only' in params:
            query_params.append(('failedOnly', params['failed_only']))  # noqa: E501
        if 'event_category' in params:
            query_params.append(('eventCategory', params['event_category']))  # noqa: E501
        if 'include_response_content' in params:
            query_params.append(('includeResponseContent', params['include_response_content']))  # noqa: E501

        header_params = {}
        if 'accept_encoding' in params:
            header_params['Accept-Encoding'] = params['accept_encoding']  # noqa: E501
        if 'content_encoding' in params:
            header_params['Content-Encoding'] = params['content_encoding']  # noqa: E501
        if 'authorization' in params:
            header_params['Authorization'] = params['authorization']  # noqa: E501
        if 'zuora_track_id' in params:
            header_params['Zuora-Track-Id'] = params['zuora_track_id']  # noqa: E501
        if 'zuora_entity_ids' in params:
            header_params['Zuora-Entity-Ids'] = params['zuora_entity_ids']  # noqa: E501

        form_params = []
        local_var_files = {}

        body_params = None
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/v1/notification-history/callout', 'GET',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='GETCalloutHistoryVOsType',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def g_et_email_history(self, **kwargs):  # noqa: E501
        """List email notification histories  # noqa: E501

        Describes how to get a notification history for notification emails.   ### Notes Request parameters and their values may be appended with a \"?\" following the HTTPS GET request.  Additional request parameter are separated by \"&\".   For example:  `GET https://rest.zuora.com/v1/notification-history/email?startTime=2015-01-12T00:00:00&endTime=2015-01-15T00:00:00&failedOnly=false&eventCategory=1000&pageSize=1`   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_email_history(async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param int page: The index number of the page that you want to retrieve. This parameter is dependent on `pageSize`. You must set `pageSize` before specifying `page`. For example, if you set `pageSize` to `20` and `page` to `2`, the 21st to 40th records are returned in the response. 
        :param int page_size: The number of records returned per page in the response. 
        :param str account_id: ID of an account. By specifying this query parameter, you can filter email notification histories by account.
        :param datetime end_time: The end date and time of records to be returned. Defaults to now. Use format yyyy-MM-ddTHH:mm:ss. The maximum date range (endTime - startTime) is three days.
        :param datetime start_time: The initial date and time of records to be returned. Defaults to (end time - 1 day). Use format yyyy-MM-ddTHH:mm:ss. The maximum date range (endTime - startTime) is three days.
        :param str object_id: The Id of an object that triggered an email notification. 
        :param bool failed_only: If `true`, only returns failed records. When `false`, returns all records in the given date range. Defaults to `true` when not specified. 
        :param float event_category: Category of records to be returned by event category.  The following formats are supported: * `{Event Type Namespace}:{Event Type Name}` if the Custom Events feature is enabled in your tenant. For example: `user.notification:NewSubscriptionCreated`. * Numeric code of the event category if the Custom Events feature is not enabled in your tenant. For example, `1210`. See [Event Category Code](https://knowledgecenter.zuora.com/DC_Developers/AA_REST_API/Event_Categories_for_Notification_Histories) for more information. 
        :return: GETEmailHistoryVOsType
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.g_et_email_history_with_http_info(**kwargs)  # noqa: E501
        else:
            (data) = self.g_et_email_history_with_http_info(**kwargs)  # noqa: E501
            return data

    def g_et_email_history_with_http_info(self, **kwargs):  # noqa: E501
        """List email notification histories  # noqa: E501

        Describes how to get a notification history for notification emails.   ### Notes Request parameters and their values may be appended with a \"?\" following the HTTPS GET request.  Additional request parameter are separated by \"&\".   For example:  `GET https://rest.zuora.com/v1/notification-history/email?startTime=2015-01-12T00:00:00&endTime=2015-01-15T00:00:00&failedOnly=false&eventCategory=1000&pageSize=1`   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_email_history_with_http_info(async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param int page: The index number of the page that you want to retrieve. This parameter is dependent on `pageSize`. You must set `pageSize` before specifying `page`. For example, if you set `pageSize` to `20` and `page` to `2`, the 21st to 40th records are returned in the response. 
        :param int page_size: The number of records returned per page in the response. 
        :param str account_id: ID of an account. By specifying this query parameter, you can filter email notification histories by account.
        :param datetime end_time: The end date and time of records to be returned. Defaults to now. Use format yyyy-MM-ddTHH:mm:ss. The maximum date range (endTime - startTime) is three days.
        :param datetime start_time: The initial date and time of records to be returned. Defaults to (end time - 1 day). Use format yyyy-MM-ddTHH:mm:ss. The maximum date range (endTime - startTime) is three days.
        :param str object_id: The Id of an object that triggered an email notification. 
        :param bool failed_only: If `true`, only returns failed records. When `false`, returns all records in the given date range. Defaults to `true` when not specified. 
        :param float event_category: Category of records to be returned by event category.  The following formats are supported: * `{Event Type Namespace}:{Event Type Name}` if the Custom Events feature is enabled in your tenant. For example: `user.notification:NewSubscriptionCreated`. * Numeric code of the event category if the Custom Events feature is not enabled in your tenant. For example, `1210`. See [Event Category Code](https://knowledgecenter.zuora.com/DC_Developers/AA_REST_API/Event_Categories_for_Notification_Histories) for more information. 
        :return: GETEmailHistoryVOsType
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids', 'page', 'page_size', 'account_id', 'end_time', 'start_time', 'object_id', 'failed_only', 'event_category']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method g_et_email_history" % key
                )
            params[key] = val
        del params['kwargs']

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `g_et_email_history`, length must be less than or equal to `64`")  # noqa: E501
        if self.api_client.client_side_validation and ('page' in params and params['page'] < 1):  # noqa: E501
            raise ValueError("Invalid value for parameter `page` when calling `g_et_email_history`, must be a value greater than or equal to `1`")  # noqa: E501
        if self.api_client.client_side_validation and ('page_size' in params and params['page_size'] > 40):  # noqa: E501
            raise ValueError("Invalid value for parameter `page_size` when calling `g_et_email_history`, must be a value less than or equal to `40`")  # noqa: E501
        collection_formats = {}

        path_params = {}

        query_params = []
        if 'page' in params:
            query_params.append(('page', params['page']))  # noqa: E501
        if 'page_size' in params:
            query_params.append(('pageSize', params['page_size']))  # noqa: E501
        if 'account_id' in params:
            query_params.append(('accountId', params['account_id']))  # noqa: E501
        if 'end_time' in params:
            query_params.append(('endTime', params['end_time']))  # noqa: E501
        if 'start_time' in params:
            query_params.append(('startTime', params['start_time']))  # noqa: E501
        if 'object_id' in params:
            query_params.append(('objectId', params['object_id']))  # noqa: E501
        if 'failed_only' in params:
            query_params.append(('failedOnly', params['failed_only']))  # noqa: E501
        if 'event_category' in params:
            query_params.append(('eventCategory', params['event_category']))  # noqa: E501

        header_params = {}
        if 'accept_encoding' in params:
            header_params['Accept-Encoding'] = params['accept_encoding']  # noqa: E501
        if 'content_encoding' in params:
            header_params['Content-Encoding'] = params['content_encoding']  # noqa: E501
        if 'authorization' in params:
            header_params['Authorization'] = params['authorization']  # noqa: E501
        if 'zuora_track_id' in params:
            header_params['Zuora-Track-Id'] = params['zuora_track_id']  # noqa: E501
        if 'zuora_entity_ids' in params:
            header_params['Zuora-Entity-Ids'] = params['zuora_entity_ids']  # noqa: E501

        form_params = []
        local_var_files = {}

        body_params = None
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/v1/notification-history/email', 'GET',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='GETEmailHistoryVOsType',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def g_et_get_email_template(self, authorization, id, **kwargs):  # noqa: E501
        """Retrieve an email template  # noqa: E501

        Queries the email template based on the specified ID. This operation supports retrieving the email template for all event types.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_get_email_template(authorization, id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param str id: The ID of the email template. (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: GETPublicEmailTemplateResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.g_et_get_email_template_with_http_info(authorization, id, **kwargs)  # noqa: E501
        else:
            (data) = self.g_et_get_email_template_with_http_info(authorization, id, **kwargs)  # noqa: E501
            return data

    def g_et_get_email_template_with_http_info(self, authorization, id, **kwargs):  # noqa: E501
        """Retrieve an email template  # noqa: E501

        Queries the email template based on the specified ID. This operation supports retrieving the email template for all event types.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_get_email_template_with_http_info(authorization, id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param str id: The ID of the email template. (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: GETPublicEmailTemplateResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['authorization', 'id', 'accept_encoding', 'content_encoding', 'zuora_track_id', 'zuora_entity_ids']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method g_et_get_email_template" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'authorization' is set
        if self.api_client.client_side_validation and ('authorization' not in params or
                                                       params['authorization'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `authorization` when calling `g_et_get_email_template`")  # noqa: E501
        # verify the required parameter 'id' is set
        if self.api_client.client_side_validation and ('id' not in params or
                                                       params['id'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `id` when calling `g_et_get_email_template`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `g_et_get_email_template`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'id' in params:
            path_params['id'] = params['id']  # noqa: E501

        query_params = []

        header_params = {}
        if 'accept_encoding' in params:
            header_params['Accept-Encoding'] = params['accept_encoding']  # noqa: E501
        if 'content_encoding' in params:
            header_params['Content-Encoding'] = params['content_encoding']  # noqa: E501
        if 'authorization' in params:
            header_params['Authorization'] = params['authorization']  # noqa: E501
        if 'zuora_track_id' in params:
            header_params['Zuora-Track-Id'] = params['zuora_track_id']  # noqa: E501
        if 'zuora_entity_ids' in params:
            header_params['Zuora-Entity-Ids'] = params['zuora_entity_ids']  # noqa: E501

        form_params = []
        local_var_files = {}

        body_params = None
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/notifications/email-templates/{id}', 'GET',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='GETPublicEmailTemplateResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def g_et_get_notification_definition(self, authorization, id, **kwargs):  # noqa: E501
        """Retrieve a notification definition  # noqa: E501

        Queries the notification definition of the given ID.  **Note**: This operation is only applicable to notifications for custom events and custom scheduled events. You can use the Settings API to retrieve notifications for standard events. For more information, see [Get a notification definition](https://knowledgecenter.zuora.com/Zuora_Central_Platform/API/BB_C_Settings_API/Settings_API_tutorials/Get_a_notification_definition).   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_get_notification_definition(authorization, id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param str id: The ID of the notification definition. (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: GETPublicNotificationDefinitionResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.g_et_get_notification_definition_with_http_info(authorization, id, **kwargs)  # noqa: E501
        else:
            (data) = self.g_et_get_notification_definition_with_http_info(authorization, id, **kwargs)  # noqa: E501
            return data

    def g_et_get_notification_definition_with_http_info(self, authorization, id, **kwargs):  # noqa: E501
        """Retrieve a notification definition  # noqa: E501

        Queries the notification definition of the given ID.  **Note**: This operation is only applicable to notifications for custom events and custom scheduled events. You can use the Settings API to retrieve notifications for standard events. For more information, see [Get a notification definition](https://knowledgecenter.zuora.com/Zuora_Central_Platform/API/BB_C_Settings_API/Settings_API_tutorials/Get_a_notification_definition).   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_get_notification_definition_with_http_info(authorization, id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param str id: The ID of the notification definition. (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: GETPublicNotificationDefinitionResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['authorization', 'id', 'accept_encoding', 'content_encoding', 'zuora_track_id', 'zuora_entity_ids']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method g_et_get_notification_definition" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'authorization' is set
        if self.api_client.client_side_validation and ('authorization' not in params or
                                                       params['authorization'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `authorization` when calling `g_et_get_notification_definition`")  # noqa: E501
        # verify the required parameter 'id' is set
        if self.api_client.client_side_validation and ('id' not in params or
                                                       params['id'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `id` when calling `g_et_get_notification_definition`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `g_et_get_notification_definition`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'id' in params:
            path_params['id'] = params['id']  # noqa: E501

        query_params = []

        header_params = {}
        if 'accept_encoding' in params:
            header_params['Accept-Encoding'] = params['accept_encoding']  # noqa: E501
        if 'content_encoding' in params:
            header_params['Content-Encoding'] = params['content_encoding']  # noqa: E501
        if 'authorization' in params:
            header_params['Authorization'] = params['authorization']  # noqa: E501
        if 'zuora_track_id' in params:
            header_params['Zuora-Track-Id'] = params['zuora_track_id']  # noqa: E501
        if 'zuora_entity_ids' in params:
            header_params['Zuora-Entity-Ids'] = params['zuora_entity_ids']  # noqa: E501

        form_params = []
        local_var_files = {}

        body_params = None
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/notifications/notification-definitions/{id}', 'GET',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='GETPublicNotificationDefinitionResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def g_et_get_notification_history_deletion_task(self, authorization, id, **kwargs):  # noqa: E501
        """Retrieve a notification history deletion task  # noqa: E501

        Get the notification history deletion task by ID.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_get_notification_history_deletion_task(authorization, id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param str id: The ID of the notification history deletion task. You can get the deletion task ID from the 202 response body of the [Delete notification histories for an account](https://www.zuora.com/developer/api-references/api/operation/DELETE_Delete_Notification_History_For_Account) operation.  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: NotificationsHistoryDeletionTaskResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.g_et_get_notification_history_deletion_task_with_http_info(authorization, id, **kwargs)  # noqa: E501
        else:
            (data) = self.g_et_get_notification_history_deletion_task_with_http_info(authorization, id, **kwargs)  # noqa: E501
            return data

    def g_et_get_notification_history_deletion_task_with_http_info(self, authorization, id, **kwargs):  # noqa: E501
        """Retrieve a notification history deletion task  # noqa: E501

        Get the notification history deletion task by ID.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_get_notification_history_deletion_task_with_http_info(authorization, id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param str id: The ID of the notification history deletion task. You can get the deletion task ID from the 202 response body of the [Delete notification histories for an account](https://www.zuora.com/developer/api-references/api/operation/DELETE_Delete_Notification_History_For_Account) operation.  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: NotificationsHistoryDeletionTaskResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['authorization', 'id', 'accept_encoding', 'content_encoding', 'zuora_track_id', 'zuora_entity_ids']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method g_et_get_notification_history_deletion_task" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'authorization' is set
        if self.api_client.client_side_validation and ('authorization' not in params or
                                                       params['authorization'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `authorization` when calling `g_et_get_notification_history_deletion_task`")  # noqa: E501
        # verify the required parameter 'id' is set
        if self.api_client.client_side_validation and ('id' not in params or
                                                       params['id'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `id` when calling `g_et_get_notification_history_deletion_task`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `g_et_get_notification_history_deletion_task`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'id' in params:
            path_params['id'] = params['id']  # noqa: E501

        query_params = []

        header_params = {}
        if 'accept_encoding' in params:
            header_params['Accept-Encoding'] = params['accept_encoding']  # noqa: E501
        if 'content_encoding' in params:
            header_params['Content-Encoding'] = params['content_encoding']  # noqa: E501
        if 'authorization' in params:
            header_params['Authorization'] = params['authorization']  # noqa: E501
        if 'zuora_track_id' in params:
            header_params['Zuora-Track-Id'] = params['zuora_track_id']  # noqa: E501
        if 'zuora_entity_ids' in params:
            header_params['Zuora-Entity-Ids'] = params['zuora_entity_ids']  # noqa: E501

        form_params = []
        local_var_files = {}

        body_params = None
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/notifications/history/tasks/{id}', 'GET',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='NotificationsHistoryDeletionTaskResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def g_et_query_email_templates(self, authorization, **kwargs):  # noqa: E501
        """List email templates  # noqa: E501

        Queries email templates. This operation supports querying email templates for all event types.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_query_email_templates(authorization, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param int start: The first index of the query result.
        :param int limit: The maximum number of results the query should return.
        :param float event_category: The event category code for standard events.
        :param str event_type_name: The name of the custom event or custom scheduled event.
        :param str name: The name of the email template.
        :return: InlineResponse2004
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.g_et_query_email_templates_with_http_info(authorization, **kwargs)  # noqa: E501
        else:
            (data) = self.g_et_query_email_templates_with_http_info(authorization, **kwargs)  # noqa: E501
            return data

    def g_et_query_email_templates_with_http_info(self, authorization, **kwargs):  # noqa: E501
        """List email templates  # noqa: E501

        Queries email templates. This operation supports querying email templates for all event types.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_query_email_templates_with_http_info(authorization, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param int start: The first index of the query result.
        :param int limit: The maximum number of results the query should return.
        :param float event_category: The event category code for standard events.
        :param str event_type_name: The name of the custom event or custom scheduled event.
        :param str name: The name of the email template.
        :return: InlineResponse2004
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['authorization', 'accept_encoding', 'content_encoding', 'zuora_track_id', 'zuora_entity_ids', 'start', 'limit', 'event_category', 'event_type_name', 'name']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method g_et_query_email_templates" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'authorization' is set
        if self.api_client.client_side_validation and ('authorization' not in params or
                                                       params['authorization'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `authorization` when calling `g_et_query_email_templates`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `g_et_query_email_templates`, length must be less than or equal to `64`")  # noqa: E501
        if self.api_client.client_side_validation and ('limit' in params and params['limit'] > 100):  # noqa: E501
            raise ValueError("Invalid value for parameter `limit` when calling `g_et_query_email_templates`, must be a value less than or equal to `100`")  # noqa: E501
        if self.api_client.client_side_validation and ('limit' in params and params['limit'] < 1):  # noqa: E501
            raise ValueError("Invalid value for parameter `limit` when calling `g_et_query_email_templates`, must be a value greater than or equal to `1`")  # noqa: E501
        collection_formats = {}

        path_params = {}

        query_params = []
        if 'start' in params:
            query_params.append(('start', params['start']))  # noqa: E501
        if 'limit' in params:
            query_params.append(('limit', params['limit']))  # noqa: E501
        if 'event_category' in params:
            query_params.append(('eventCategory', params['event_category']))  # noqa: E501
        if 'event_type_name' in params:
            query_params.append(('eventTypeName', params['event_type_name']))  # noqa: E501
        if 'name' in params:
            query_params.append(('name', params['name']))  # noqa: E501

        header_params = {}
        if 'accept_encoding' in params:
            header_params['Accept-Encoding'] = params['accept_encoding']  # noqa: E501
        if 'content_encoding' in params:
            header_params['Content-Encoding'] = params['content_encoding']  # noqa: E501
        if 'authorization' in params:
            header_params['Authorization'] = params['authorization']  # noqa: E501
        if 'zuora_track_id' in params:
            header_params['Zuora-Track-Id'] = params['zuora_track_id']  # noqa: E501
        if 'zuora_entity_ids' in params:
            header_params['Zuora-Entity-Ids'] = params['zuora_entity_ids']  # noqa: E501

        form_params = []
        local_var_files = {}

        body_params = None
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/notifications/email-templates', 'GET',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='InlineResponse2004',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def g_et_query_notification_definitions(self, authorization, **kwargs):  # noqa: E501
        """List notification definitions  # noqa: E501

        Queries notification definitions with the specified filters.  **Note**: This operation is only applicable to notifications for custom events and custom scheduled events. You can use the Settings API to list all notifications under a particular communication profile including notificatiions for standard events. For more information, see [Get all notifications under a particular communication profile](https://knowledgecenter.zuora.com/Zuora_Central_Platform/API/BB_C_Settings_API/Settings_API_tutorials/Get_all_Notifications_under_a_particular_Communication_Profile).   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_query_notification_definitions(authorization, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param int start: The first index of the query result.
        :param int limit: The maximum number of results the query should return.
        :param str profile_id: Id of the profile.
        :param str event_type_name: The name of the event.
        :param str email_template_id: The ID of the email template.
        :return: InlineResponse2003
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.g_et_query_notification_definitions_with_http_info(authorization, **kwargs)  # noqa: E501
        else:
            (data) = self.g_et_query_notification_definitions_with_http_info(authorization, **kwargs)  # noqa: E501
            return data

    def g_et_query_notification_definitions_with_http_info(self, authorization, **kwargs):  # noqa: E501
        """List notification definitions  # noqa: E501

        Queries notification definitions with the specified filters.  **Note**: This operation is only applicable to notifications for custom events and custom scheduled events. You can use the Settings API to list all notifications under a particular communication profile including notificatiions for standard events. For more information, see [Get all notifications under a particular communication profile](https://knowledgecenter.zuora.com/Zuora_Central_Platform/API/BB_C_Settings_API/Settings_API_tutorials/Get_all_Notifications_under_a_particular_Communication_Profile).   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_query_notification_definitions_with_http_info(authorization, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param int start: The first index of the query result.
        :param int limit: The maximum number of results the query should return.
        :param str profile_id: Id of the profile.
        :param str event_type_name: The name of the event.
        :param str email_template_id: The ID of the email template.
        :return: InlineResponse2003
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['authorization', 'accept_encoding', 'content_encoding', 'zuora_track_id', 'zuora_entity_ids', 'start', 'limit', 'profile_id', 'event_type_name', 'email_template_id']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method g_et_query_notification_definitions" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'authorization' is set
        if self.api_client.client_side_validation and ('authorization' not in params or
                                                       params['authorization'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `authorization` when calling `g_et_query_notification_definitions`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `g_et_query_notification_definitions`, length must be less than or equal to `64`")  # noqa: E501
        if self.api_client.client_side_validation and ('limit' in params and params['limit'] > 100):  # noqa: E501
            raise ValueError("Invalid value for parameter `limit` when calling `g_et_query_notification_definitions`, must be a value less than or equal to `100`")  # noqa: E501
        if self.api_client.client_side_validation and ('limit' in params and params['limit'] < 1):  # noqa: E501
            raise ValueError("Invalid value for parameter `limit` when calling `g_et_query_notification_definitions`, must be a value greater than or equal to `1`")  # noqa: E501
        collection_formats = {}

        path_params = {}

        query_params = []
        if 'start' in params:
            query_params.append(('start', params['start']))  # noqa: E501
        if 'limit' in params:
            query_params.append(('limit', params['limit']))  # noqa: E501
        if 'profile_id' in params:
            query_params.append(('profileId', params['profile_id']))  # noqa: E501
        if 'event_type_name' in params:
            query_params.append(('eventTypeName', params['event_type_name']))  # noqa: E501
        if 'email_template_id' in params:
            query_params.append(('emailTemplateId', params['email_template_id']))  # noqa: E501

        header_params = {}
        if 'accept_encoding' in params:
            header_params['Accept-Encoding'] = params['accept_encoding']  # noqa: E501
        if 'content_encoding' in params:
            header_params['Content-Encoding'] = params['content_encoding']  # noqa: E501
        if 'authorization' in params:
            header_params['Authorization'] = params['authorization']  # noqa: E501
        if 'zuora_track_id' in params:
            header_params['Zuora-Track-Id'] = params['zuora_track_id']  # noqa: E501
        if 'zuora_entity_ids' in params:
            header_params['Zuora-Entity-Ids'] = params['zuora_entity_ids']  # noqa: E501

        form_params = []
        local_var_files = {}

        body_params = None
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/notifications/notification-definitions', 'GET',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='InlineResponse2003',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ost_create_email_template(self, authorization, post_public_email_template_request, **kwargs):  # noqa: E501
        """Create an email template  # noqa: E501

        Creates an email template.   This operation supports creating the email template for all event types.  - If you specify the `eventCategory` field, the email template is created based on a standard event. See [Standard Event Categories](https://knowledgecenter.zuora.com/Central_Platform/Notifications/A_Standard_Events/Standard_Event_Category_Code_for_Notification_Histories_API) for all standard event category codes. - If you specify the `eventTypeName` field, the email template is created based on the corresponding custom event or custom scheduled event. See [Custom event triggers](https://www.zuora.com/developer/api-references/api/tag/Custom-Event-Triggers/) for more information about custom events, and [Custom scheduled events](https://www.zuora.com/developer/api-references/api/tag/Custom-Scheduled-Events/) for more information about custom scheduled events.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_create_email_template(authorization, post_public_email_template_request, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param POSTPublicEmailTemplateRequest post_public_email_template_request: The request body to create an email template. (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: GETPublicEmailTemplateResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ost_create_email_template_with_http_info(authorization, post_public_email_template_request, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ost_create_email_template_with_http_info(authorization, post_public_email_template_request, **kwargs)  # noqa: E501
            return data

    def p_ost_create_email_template_with_http_info(self, authorization, post_public_email_template_request, **kwargs):  # noqa: E501
        """Create an email template  # noqa: E501

        Creates an email template.   This operation supports creating the email template for all event types.  - If you specify the `eventCategory` field, the email template is created based on a standard event. See [Standard Event Categories](https://knowledgecenter.zuora.com/Central_Platform/Notifications/A_Standard_Events/Standard_Event_Category_Code_for_Notification_Histories_API) for all standard event category codes. - If you specify the `eventTypeName` field, the email template is created based on the corresponding custom event or custom scheduled event. See [Custom event triggers](https://www.zuora.com/developer/api-references/api/tag/Custom-Event-Triggers/) for more information about custom events, and [Custom scheduled events](https://www.zuora.com/developer/api-references/api/tag/Custom-Scheduled-Events/) for more information about custom scheduled events.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_create_email_template_with_http_info(authorization, post_public_email_template_request, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param POSTPublicEmailTemplateRequest post_public_email_template_request: The request body to create an email template. (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: GETPublicEmailTemplateResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['authorization', 'post_public_email_template_request', 'idempotency_key', 'accept_encoding', 'content_encoding', 'zuora_track_id', 'zuora_entity_ids']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ost_create_email_template" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'authorization' is set
        if self.api_client.client_side_validation and ('authorization' not in params or
                                                       params['authorization'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `authorization` when calling `p_ost_create_email_template`")  # noqa: E501
        # verify the required parameter 'post_public_email_template_request' is set
        if self.api_client.client_side_validation and ('post_public_email_template_request' not in params or
                                                       params['post_public_email_template_request'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `post_public_email_template_request` when calling `p_ost_create_email_template`")  # noqa: E501

        if self.api_client.client_side_validation and ('idempotency_key' in params and
                                                       len(params['idempotency_key']) > 255):
            raise ValueError("Invalid value for parameter `idempotency_key` when calling `p_ost_create_email_template`, length must be less than or equal to `255`")  # noqa: E501
        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ost_create_email_template`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}

        query_params = []

        header_params = {}
        if 'idempotency_key' in params:
            header_params['Idempotency-Key'] = params['idempotency_key']  # noqa: E501
        if 'accept_encoding' in params:
            header_params['Accept-Encoding'] = params['accept_encoding']  # noqa: E501
        if 'content_encoding' in params:
            header_params['Content-Encoding'] = params['content_encoding']  # noqa: E501
        if 'authorization' in params:
            header_params['Authorization'] = params['authorization']  # noqa: E501
        if 'zuora_track_id' in params:
            header_params['Zuora-Track-Id'] = params['zuora_track_id']  # noqa: E501
        if 'zuora_entity_ids' in params:
            header_params['Zuora-Entity-Ids'] = params['zuora_entity_ids']  # noqa: E501

        form_params = []
        local_var_files = {}

        body_params = None
        if 'post_public_email_template_request' in params:
            body_params = params['post_public_email_template_request']
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/notifications/email-templates', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='GETPublicEmailTemplateResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ost_create_notification_definition(self, authorization, entity, **kwargs):  # noqa: E501
        """Create a notification definition  # noqa: E501

        Creates a notification definition. If a filter rule is specified, it will be evaluated to see if the notification definition is qualified to handle the incoming events  during runtime. If the notification is qualified, it will send the email and invoke the callout if it has an email template or a callout.   **Note**: This operation is only applicable to notifications for custom events and custom scheduled events.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_create_notification_definition(authorization, entity, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param POSTPublicNotificationDefinitionRequest entity: The request body used to create the notification definition. (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: GETPublicNotificationDefinitionResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ost_create_notification_definition_with_http_info(authorization, entity, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ost_create_notification_definition_with_http_info(authorization, entity, **kwargs)  # noqa: E501
            return data

    def p_ost_create_notification_definition_with_http_info(self, authorization, entity, **kwargs):  # noqa: E501
        """Create a notification definition  # noqa: E501

        Creates a notification definition. If a filter rule is specified, it will be evaluated to see if the notification definition is qualified to handle the incoming events  during runtime. If the notification is qualified, it will send the email and invoke the callout if it has an email template or a callout.   **Note**: This operation is only applicable to notifications for custom events and custom scheduled events.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_create_notification_definition_with_http_info(authorization, entity, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param POSTPublicNotificationDefinitionRequest entity: The request body used to create the notification definition. (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: GETPublicNotificationDefinitionResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['authorization', 'entity', 'idempotency_key', 'accept_encoding', 'content_encoding', 'zuora_track_id', 'zuora_entity_ids']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ost_create_notification_definition" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'authorization' is set
        if self.api_client.client_side_validation and ('authorization' not in params or
                                                       params['authorization'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `authorization` when calling `p_ost_create_notification_definition`")  # noqa: E501
        # verify the required parameter 'entity' is set
        if self.api_client.client_side_validation and ('entity' not in params or
                                                       params['entity'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `entity` when calling `p_ost_create_notification_definition`")  # noqa: E501

        if self.api_client.client_side_validation and ('idempotency_key' in params and
                                                       len(params['idempotency_key']) > 255):
            raise ValueError("Invalid value for parameter `idempotency_key` when calling `p_ost_create_notification_definition`, length must be less than or equal to `255`")  # noqa: E501
        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ost_create_notification_definition`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}

        query_params = []

        header_params = {}
        if 'idempotency_key' in params:
            header_params['Idempotency-Key'] = params['idempotency_key']  # noqa: E501
        if 'accept_encoding' in params:
            header_params['Accept-Encoding'] = params['accept_encoding']  # noqa: E501
        if 'content_encoding' in params:
            header_params['Content-Encoding'] = params['content_encoding']  # noqa: E501
        if 'authorization' in params:
            header_params['Authorization'] = params['authorization']  # noqa: E501
        if 'zuora_track_id' in params:
            header_params['Zuora-Track-Id'] = params['zuora_track_id']  # noqa: E501
        if 'zuora_entity_ids' in params:
            header_params['Zuora-Entity-Ids'] = params['zuora_entity_ids']  # noqa: E501

        form_params = []
        local_var_files = {}

        body_params = None
        if 'entity' in params:
            body_params = params['entity']
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/notifications/notification-definitions', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='GETPublicNotificationDefinitionResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ost_create_or_update_email_templates(self, authorization, post_create_or_update_email_template_request, **kwargs):  # noqa: E501
        """Create or update email templates  # noqa: E501

        Creates email templates for standard or custom events if you do not specify email template IDs, or updates existing email templates if you specify valid email template IDs.  For each email template when you are creating email templates, whether the template is created for a standard event, a custom event, or a custom scheduled event is dependent on whether you specify the `eventCategory` or `eventTypeName` field.  - If you specify the `eventCategory` field, the email template is created based on a standard event. See [Standard Event Categories](https://knowledgecenter.zuora.com/Central_Platform/Notifications/A_Standard_Events/Standard_Event_Category_Code_for_Notification_Histories_API) for all standard event category codes. - If you specify the `eventTypeName` field, the email template is created based on the corresponding custom event or custom scheduled event. See [Custom event triggers](https://www.zuora.com/developer/api-references/api/tag/Custom-Event-Triggers/) for more information about custom events, and [Custom scheduled events](https://www.zuora.com/developer/api-references/api/tag/Custom-Scheduled-Events/) for more information about custom scheduled events.  The maximum number of email templates that you can create or update by one call is 1,000.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_create_or_update_email_templates(authorization, post_create_or_update_email_template_request, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param POSTCreateOrUpdateEmailTemplateRequest post_create_or_update_email_template_request: The request body to import or update email templates. (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: CreateOrUpdateEmailTemplatesResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ost_create_or_update_email_templates_with_http_info(authorization, post_create_or_update_email_template_request, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ost_create_or_update_email_templates_with_http_info(authorization, post_create_or_update_email_template_request, **kwargs)  # noqa: E501
            return data

    def p_ost_create_or_update_email_templates_with_http_info(self, authorization, post_create_or_update_email_template_request, **kwargs):  # noqa: E501
        """Create or update email templates  # noqa: E501

        Creates email templates for standard or custom events if you do not specify email template IDs, or updates existing email templates if you specify valid email template IDs.  For each email template when you are creating email templates, whether the template is created for a standard event, a custom event, or a custom scheduled event is dependent on whether you specify the `eventCategory` or `eventTypeName` field.  - If you specify the `eventCategory` field, the email template is created based on a standard event. See [Standard Event Categories](https://knowledgecenter.zuora.com/Central_Platform/Notifications/A_Standard_Events/Standard_Event_Category_Code_for_Notification_Histories_API) for all standard event category codes. - If you specify the `eventTypeName` field, the email template is created based on the corresponding custom event or custom scheduled event. See [Custom event triggers](https://www.zuora.com/developer/api-references/api/tag/Custom-Event-Triggers/) for more information about custom events, and [Custom scheduled events](https://www.zuora.com/developer/api-references/api/tag/Custom-Scheduled-Events/) for more information about custom scheduled events.  The maximum number of email templates that you can create or update by one call is 1,000.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_create_or_update_email_templates_with_http_info(authorization, post_create_or_update_email_template_request, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param POSTCreateOrUpdateEmailTemplateRequest post_create_or_update_email_template_request: The request body to import or update email templates. (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: CreateOrUpdateEmailTemplatesResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['authorization', 'post_create_or_update_email_template_request', 'idempotency_key', 'accept_encoding', 'content_encoding', 'zuora_track_id', 'zuora_entity_ids']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ost_create_or_update_email_templates" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'authorization' is set
        if self.api_client.client_side_validation and ('authorization' not in params or
                                                       params['authorization'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `authorization` when calling `p_ost_create_or_update_email_templates`")  # noqa: E501
        # verify the required parameter 'post_create_or_update_email_template_request' is set
        if self.api_client.client_side_validation and ('post_create_or_update_email_template_request' not in params or
                                                       params['post_create_or_update_email_template_request'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `post_create_or_update_email_template_request` when calling `p_ost_create_or_update_email_templates`")  # noqa: E501

        if self.api_client.client_side_validation and ('idempotency_key' in params and
                                                       len(params['idempotency_key']) > 255):
            raise ValueError("Invalid value for parameter `idempotency_key` when calling `p_ost_create_or_update_email_templates`, length must be less than or equal to `255`")  # noqa: E501
        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ost_create_or_update_email_templates`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}

        query_params = []

        header_params = {}
        if 'idempotency_key' in params:
            header_params['Idempotency-Key'] = params['idempotency_key']  # noqa: E501
        if 'accept_encoding' in params:
            header_params['Accept-Encoding'] = params['accept_encoding']  # noqa: E501
        if 'content_encoding' in params:
            header_params['Content-Encoding'] = params['content_encoding']  # noqa: E501
        if 'authorization' in params:
            header_params['Authorization'] = params['authorization']  # noqa: E501
        if 'zuora_track_id' in params:
            header_params['Zuora-Track-Id'] = params['zuora_track_id']  # noqa: E501
        if 'zuora_entity_ids' in params:
            header_params['Zuora-Entity-Ids'] = params['zuora_entity_ids']  # noqa: E501

        form_params = []
        local_var_files = {}

        body_params = None
        if 'post_create_or_update_email_template_request' in params:
            body_params = params['post_create_or_update_email_template_request']
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/notifications/email-templates/import', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='CreateOrUpdateEmailTemplatesResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ost_resend_callout_notifications(self, authorization, post_resend_callout_notifications, **kwargs):  # noqa: E501
        """Resend callout notifications  # noqa: E501

        Resends callout notifications if your customers did not receive previous callout notifications.  Details about the status codes and response contents of this operation are as follows:  | Scenario                               | Status code     | Response content                                            | |----------------------------------------|-----------------|-------------------------------------------------------------| | Success for all notifications          | 202 Accepted    | (blank)                                                     | | Success for at least one notification  | 202 Accepted    | Error code and error message of each failed notification    | | Failure for all notifications          | 400 Bad Request | Error code and error message of each failed notification    |   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_resend_callout_notifications(authorization, post_resend_callout_notifications, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param POSTResendCalloutNotifications post_resend_callout_notifications: The request body to resend callout notifications. (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: ResendCalloutNotificationsFailedResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ost_resend_callout_notifications_with_http_info(authorization, post_resend_callout_notifications, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ost_resend_callout_notifications_with_http_info(authorization, post_resend_callout_notifications, **kwargs)  # noqa: E501
            return data

    def p_ost_resend_callout_notifications_with_http_info(self, authorization, post_resend_callout_notifications, **kwargs):  # noqa: E501
        """Resend callout notifications  # noqa: E501

        Resends callout notifications if your customers did not receive previous callout notifications.  Details about the status codes and response contents of this operation are as follows:  | Scenario                               | Status code     | Response content                                            | |----------------------------------------|-----------------|-------------------------------------------------------------| | Success for all notifications          | 202 Accepted    | (blank)                                                     | | Success for at least one notification  | 202 Accepted    | Error code and error message of each failed notification    | | Failure for all notifications          | 400 Bad Request | Error code and error message of each failed notification    |   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_resend_callout_notifications_with_http_info(authorization, post_resend_callout_notifications, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param POSTResendCalloutNotifications post_resend_callout_notifications: The request body to resend callout notifications. (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: ResendCalloutNotificationsFailedResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['authorization', 'post_resend_callout_notifications', 'idempotency_key', 'accept_encoding', 'content_encoding', 'zuora_track_id', 'zuora_entity_ids']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ost_resend_callout_notifications" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'authorization' is set
        if self.api_client.client_side_validation and ('authorization' not in params or
                                                       params['authorization'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `authorization` when calling `p_ost_resend_callout_notifications`")  # noqa: E501
        # verify the required parameter 'post_resend_callout_notifications' is set
        if self.api_client.client_side_validation and ('post_resend_callout_notifications' not in params or
                                                       params['post_resend_callout_notifications'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `post_resend_callout_notifications` when calling `p_ost_resend_callout_notifications`")  # noqa: E501

        if self.api_client.client_side_validation and ('idempotency_key' in params and
                                                       len(params['idempotency_key']) > 255):
            raise ValueError("Invalid value for parameter `idempotency_key` when calling `p_ost_resend_callout_notifications`, length must be less than or equal to `255`")  # noqa: E501
        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ost_resend_callout_notifications`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}

        query_params = []

        header_params = {}
        if 'idempotency_key' in params:
            header_params['Idempotency-Key'] = params['idempotency_key']  # noqa: E501
        if 'accept_encoding' in params:
            header_params['Accept-Encoding'] = params['accept_encoding']  # noqa: E501
        if 'content_encoding' in params:
            header_params['Content-Encoding'] = params['content_encoding']  # noqa: E501
        if 'authorization' in params:
            header_params['Authorization'] = params['authorization']  # noqa: E501
        if 'zuora_track_id' in params:
            header_params['Zuora-Track-Id'] = params['zuora_track_id']  # noqa: E501
        if 'zuora_entity_ids' in params:
            header_params['Zuora-Entity-Ids'] = params['zuora_entity_ids']  # noqa: E501

        form_params = []
        local_var_files = {}

        body_params = None
        if 'post_resend_callout_notifications' in params:
            body_params = params['post_resend_callout_notifications']
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/notifications/callout-histories/resend', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='ResendCalloutNotificationsFailedResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ost_resend_email_notifications(self, authorization, post_resend_email_notifications, **kwargs):  # noqa: E501
        """Resend email notifications  # noqa: E501

        Resends email notifications if your customers did not receive previous email notifications.  Details about the status codes and response contents of this operation are as follows:  | Scenario                               | Status code     | Response content                                            | |----------------------------------------|-----------------|-------------------------------------------------------------| | Success for all notifications          | 202 Accepted    | (blank)                                                     | | Success for at least one notification  | 202 Accepted    | Error code and error message of each failed notification    | | Failure for all notifications          | 400 Bad Request | Error code and error message of each failed notification    |   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_resend_email_notifications(authorization, post_resend_email_notifications, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param POSTResendEmailNotifications post_resend_email_notifications: The request body to resend email notifications. (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: ResendEmailNotificationsFailedResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ost_resend_email_notifications_with_http_info(authorization, post_resend_email_notifications, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ost_resend_email_notifications_with_http_info(authorization, post_resend_email_notifications, **kwargs)  # noqa: E501
            return data

    def p_ost_resend_email_notifications_with_http_info(self, authorization, post_resend_email_notifications, **kwargs):  # noqa: E501
        """Resend email notifications  # noqa: E501

        Resends email notifications if your customers did not receive previous email notifications.  Details about the status codes and response contents of this operation are as follows:  | Scenario                               | Status code     | Response content                                            | |----------------------------------------|-----------------|-------------------------------------------------------------| | Success for all notifications          | 202 Accepted    | (blank)                                                     | | Success for at least one notification  | 202 Accepted    | Error code and error message of each failed notification    | | Failure for all notifications          | 400 Bad Request | Error code and error message of each failed notification    |   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_resend_email_notifications_with_http_info(authorization, post_resend_email_notifications, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param POSTResendEmailNotifications post_resend_email_notifications: The request body to resend email notifications. (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: ResendEmailNotificationsFailedResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['authorization', 'post_resend_email_notifications', 'idempotency_key', 'accept_encoding', 'content_encoding', 'zuora_track_id', 'zuora_entity_ids']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ost_resend_email_notifications" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'authorization' is set
        if self.api_client.client_side_validation and ('authorization' not in params or
                                                       params['authorization'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `authorization` when calling `p_ost_resend_email_notifications`")  # noqa: E501
        # verify the required parameter 'post_resend_email_notifications' is set
        if self.api_client.client_side_validation and ('post_resend_email_notifications' not in params or
                                                       params['post_resend_email_notifications'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `post_resend_email_notifications` when calling `p_ost_resend_email_notifications`")  # noqa: E501

        if self.api_client.client_side_validation and ('idempotency_key' in params and
                                                       len(params['idempotency_key']) > 255):
            raise ValueError("Invalid value for parameter `idempotency_key` when calling `p_ost_resend_email_notifications`, length must be less than or equal to `255`")  # noqa: E501
        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ost_resend_email_notifications`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}

        query_params = []

        header_params = {}
        if 'idempotency_key' in params:
            header_params['Idempotency-Key'] = params['idempotency_key']  # noqa: E501
        if 'accept_encoding' in params:
            header_params['Accept-Encoding'] = params['accept_encoding']  # noqa: E501
        if 'content_encoding' in params:
            header_params['Content-Encoding'] = params['content_encoding']  # noqa: E501
        if 'authorization' in params:
            header_params['Authorization'] = params['authorization']  # noqa: E501
        if 'zuora_track_id' in params:
            header_params['Zuora-Track-Id'] = params['zuora_track_id']  # noqa: E501
        if 'zuora_entity_ids' in params:
            header_params['Zuora-Entity-Ids'] = params['zuora_entity_ids']  # noqa: E501

        form_params = []
        local_var_files = {}

        body_params = None
        if 'post_resend_email_notifications' in params:
            body_params = params['post_resend_email_notifications']
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/notifications/email-histories/resend', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='ResendEmailNotificationsFailedResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ut_update_email_template(self, authorization, id, put_public_email_template_request, **kwargs):  # noqa: E501
        """Update an email template  # noqa: E501

        Updates an email template. This operation supports updating the email template for all event types.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ut_update_email_template(authorization, id, put_public_email_template_request, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param str id: The ID of the email template to be updated. (required)
        :param PUTPublicEmailTemplateRequest put_public_email_template_request: The request body to update an email template. (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: GETPublicEmailTemplateResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ut_update_email_template_with_http_info(authorization, id, put_public_email_template_request, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ut_update_email_template_with_http_info(authorization, id, put_public_email_template_request, **kwargs)  # noqa: E501
            return data

    def p_ut_update_email_template_with_http_info(self, authorization, id, put_public_email_template_request, **kwargs):  # noqa: E501
        """Update an email template  # noqa: E501

        Updates an email template. This operation supports updating the email template for all event types.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ut_update_email_template_with_http_info(authorization, id, put_public_email_template_request, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param str id: The ID of the email template to be updated. (required)
        :param PUTPublicEmailTemplateRequest put_public_email_template_request: The request body to update an email template. (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: GETPublicEmailTemplateResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['authorization', 'id', 'put_public_email_template_request', 'accept_encoding', 'content_encoding', 'zuora_track_id', 'zuora_entity_ids']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ut_update_email_template" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'authorization' is set
        if self.api_client.client_side_validation and ('authorization' not in params or
                                                       params['authorization'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `authorization` when calling `p_ut_update_email_template`")  # noqa: E501
        # verify the required parameter 'id' is set
        if self.api_client.client_side_validation and ('id' not in params or
                                                       params['id'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `id` when calling `p_ut_update_email_template`")  # noqa: E501
        # verify the required parameter 'put_public_email_template_request' is set
        if self.api_client.client_side_validation and ('put_public_email_template_request' not in params or
                                                       params['put_public_email_template_request'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `put_public_email_template_request` when calling `p_ut_update_email_template`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ut_update_email_template`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'id' in params:
            path_params['id'] = params['id']  # noqa: E501

        query_params = []

        header_params = {}
        if 'accept_encoding' in params:
            header_params['Accept-Encoding'] = params['accept_encoding']  # noqa: E501
        if 'content_encoding' in params:
            header_params['Content-Encoding'] = params['content_encoding']  # noqa: E501
        if 'authorization' in params:
            header_params['Authorization'] = params['authorization']  # noqa: E501
        if 'zuora_track_id' in params:
            header_params['Zuora-Track-Id'] = params['zuora_track_id']  # noqa: E501
        if 'zuora_entity_ids' in params:
            header_params['Zuora-Entity-Ids'] = params['zuora_entity_ids']  # noqa: E501

        form_params = []
        local_var_files = {}

        body_params = None
        if 'put_public_email_template_request' in params:
            body_params = params['put_public_email_template_request']
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/notifications/email-templates/{id}', 'PUT',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='GETPublicEmailTemplateResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ut_update_notification_definition(self, authorization, id, put_public_notification_definition_request, **kwargs):  # noqa: E501
        """Update a notification definition  # noqa: E501

        Updates a notification definition.  **Note**: This operation is only applicable to notifications for custom events and custom scheduled events. You can use the Settings API to update notifications for standard events. For more information, see [Update a notification definition](https://knowledgecenter.zuora.com/Zuora_Central_Platform/API/BB_C_Settings_API/Settings_API_tutorials/Update_a_notification_definition).   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ut_update_notification_definition(authorization, id, put_public_notification_definition_request, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param str id: The ID of the notification definition to be updated. (required)
        :param PUTPublicNotificationDefinitionRequest put_public_notification_definition_request: The request body of the notification definition to be updated. (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: GETPublicNotificationDefinitionResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ut_update_notification_definition_with_http_info(authorization, id, put_public_notification_definition_request, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ut_update_notification_definition_with_http_info(authorization, id, put_public_notification_definition_request, **kwargs)  # noqa: E501
            return data

    def p_ut_update_notification_definition_with_http_info(self, authorization, id, put_public_notification_definition_request, **kwargs):  # noqa: E501
        """Update a notification definition  # noqa: E501

        Updates a notification definition.  **Note**: This operation is only applicable to notifications for custom events and custom scheduled events. You can use the Settings API to update notifications for standard events. For more information, see [Update a notification definition](https://knowledgecenter.zuora.com/Zuora_Central_Platform/API/BB_C_Settings_API/Settings_API_tutorials/Update_a_notification_definition).   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ut_update_notification_definition_with_http_info(authorization, id, put_public_notification_definition_request, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken).  (required)
        :param str id: The ID of the notification definition to be updated. (required)
        :param PUTPublicNotificationDefinitionRequest put_public_notification_definition_request: The request body of the notification definition to be updated. (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: GETPublicNotificationDefinitionResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['authorization', 'id', 'put_public_notification_definition_request', 'accept_encoding', 'content_encoding', 'zuora_track_id', 'zuora_entity_ids']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ut_update_notification_definition" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'authorization' is set
        if self.api_client.client_side_validation and ('authorization' not in params or
                                                       params['authorization'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `authorization` when calling `p_ut_update_notification_definition`")  # noqa: E501
        # verify the required parameter 'id' is set
        if self.api_client.client_side_validation and ('id' not in params or
                                                       params['id'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `id` when calling `p_ut_update_notification_definition`")  # noqa: E501
        # verify the required parameter 'put_public_notification_definition_request' is set
        if self.api_client.client_side_validation and ('put_public_notification_definition_request' not in params or
                                                       params['put_public_notification_definition_request'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `put_public_notification_definition_request` when calling `p_ut_update_notification_definition`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ut_update_notification_definition`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'id' in params:
            path_params['id'] = params['id']  # noqa: E501

        query_params = []

        header_params = {}
        if 'accept_encoding' in params:
            header_params['Accept-Encoding'] = params['accept_encoding']  # noqa: E501
        if 'content_encoding' in params:
            header_params['Content-Encoding'] = params['content_encoding']  # noqa: E501
        if 'authorization' in params:
            header_params['Authorization'] = params['authorization']  # noqa: E501
        if 'zuora_track_id' in params:
            header_params['Zuora-Track-Id'] = params['zuora_track_id']  # noqa: E501
        if 'zuora_entity_ids' in params:
            header_params['Zuora-Entity-Ids'] = params['zuora_entity_ids']  # noqa: E501

        form_params = []
        local_var_files = {}

        body_params = None
        if 'put_public_notification_definition_request' in params:
            body_params = params['put_public_notification_definition_request']
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/notifications/notification-definitions/{id}', 'PUT',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='GETPublicNotificationDefinitionResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)
