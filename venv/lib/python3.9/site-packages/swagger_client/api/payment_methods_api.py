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


class PaymentMethodsApi(object):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    Ref: https://github.com/swagger-api/swagger-codegen
    """

    def __init__(self, api_client=None):
        if api_client is None:
            api_client = ApiClient()
        self.api_client = api_client

    def d_elete_payment_methods(self, payment_method_id, **kwargs):  # noqa: E501
        """Delete a payment method  # noqa: E501

        Deletes a credit card payment method.  If the specified payment method is the account's default payment method, the request will fail.  In that case, you must first designate a different payment method for that customer to be the default.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.d_elete_payment_methods(payment_method_id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str payment_method_id: Unique identifier of a payment method. (Since this ID is unique, and linked to a customer account in the system, no customer identifier is needed.) (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: CommonResponseType
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.d_elete_payment_methods_with_http_info(payment_method_id, **kwargs)  # noqa: E501
        else:
            (data) = self.d_elete_payment_methods_with_http_info(payment_method_id, **kwargs)  # noqa: E501
            return data

    def d_elete_payment_methods_with_http_info(self, payment_method_id, **kwargs):  # noqa: E501
        """Delete a payment method  # noqa: E501

        Deletes a credit card payment method.  If the specified payment method is the account's default payment method, the request will fail.  In that case, you must first designate a different payment method for that customer to be the default.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.d_elete_payment_methods_with_http_info(payment_method_id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str payment_method_id: Unique identifier of a payment method. (Since this ID is unique, and linked to a customer account in the system, no customer identifier is needed.) (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: CommonResponseType
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['payment_method_id', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method d_elete_payment_methods" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'payment_method_id' is set
        if self.api_client.client_side_validation and ('payment_method_id' not in params or
                                                       params['payment_method_id'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `payment_method_id` when calling `d_elete_payment_methods`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `d_elete_payment_methods`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'payment_method_id' in params:
            path_params['payment-method-id'] = params['payment_method_id']  # noqa: E501

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
            '/v1/payment-methods/{payment-method-id}', 'DELETE',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='CommonResponseType',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def g_et_payment_method(self, payment_method_id, **kwargs):  # noqa: E501
        """Retrieve a payment method  # noqa: E501

        This operation allows you to get the detailed information about a payment method.  **Note:** This operation also supports retrieving the custom payment method created through the [Open Payment Method](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/MB_Set_up_custom_payment_gateways_and_payment_methods) service.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_payment_method(payment_method_id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str payment_method_id: Unique ID of the payment method to update. (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: GETPaymentMethodResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.g_et_payment_method_with_http_info(payment_method_id, **kwargs)  # noqa: E501
        else:
            (data) = self.g_et_payment_method_with_http_info(payment_method_id, **kwargs)  # noqa: E501
            return data

    def g_et_payment_method_with_http_info(self, payment_method_id, **kwargs):  # noqa: E501
        """Retrieve a payment method  # noqa: E501

        This operation allows you to get the detailed information about a payment method.  **Note:** This operation also supports retrieving the custom payment method created through the [Open Payment Method](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/MB_Set_up_custom_payment_gateways_and_payment_methods) service.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_payment_method_with_http_info(payment_method_id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str payment_method_id: Unique ID of the payment method to update. (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: GETPaymentMethodResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['payment_method_id', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method g_et_payment_method" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'payment_method_id' is set
        if self.api_client.client_side_validation and ('payment_method_id' not in params or
                                                       params['payment_method_id'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `payment_method_id` when calling `g_et_payment_method`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `g_et_payment_method`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'payment_method_id' in params:
            path_params['payment-method-id'] = params['payment_method_id']  # noqa: E501

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
            '/v1/payment-methods/{payment-method-id}', 'GET',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='GETPaymentMethodResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def g_et_stored_credential_profiles(self, payment_method_id, **kwargs):  # noqa: E501
        """List stored credential profiles of a payment method  # noqa: E501

        Retrieves the stored credential profiles within a payment method.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_stored_credential_profiles(payment_method_id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str payment_method_id: ID of a payment method.  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param bool include_all: Specifies whether to retrieve all the stored credential profiles within the payment method.  By default, Zuora returns only the stored credential profiles with `Agreed` or `Active` status. If you set this parameter to `true`, Zuora returns all the stored credential profiles. 
        :return: GetStoredCredentialProfilesResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.g_et_stored_credential_profiles_with_http_info(payment_method_id, **kwargs)  # noqa: E501
        else:
            (data) = self.g_et_stored_credential_profiles_with_http_info(payment_method_id, **kwargs)  # noqa: E501
            return data

    def g_et_stored_credential_profiles_with_http_info(self, payment_method_id, **kwargs):  # noqa: E501
        """List stored credential profiles of a payment method  # noqa: E501

        Retrieves the stored credential profiles within a payment method.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_stored_credential_profiles_with_http_info(payment_method_id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str payment_method_id: ID of a payment method.  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param bool include_all: Specifies whether to retrieve all the stored credential profiles within the payment method.  By default, Zuora returns only the stored credential profiles with `Agreed` or `Active` status. If you set this parameter to `true`, Zuora returns all the stored credential profiles. 
        :return: GetStoredCredentialProfilesResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['payment_method_id', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids', 'include_all']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method g_et_stored_credential_profiles" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'payment_method_id' is set
        if self.api_client.client_side_validation and ('payment_method_id' not in params or
                                                       params['payment_method_id'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `payment_method_id` when calling `g_et_stored_credential_profiles`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `g_et_stored_credential_profiles`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'payment_method_id' in params:
            path_params['payment-method-id'] = params['payment_method_id']  # noqa: E501

        query_params = []
        if 'include_all' in params:
            query_params.append(('includeAll', params['include_all']))  # noqa: E501

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
            '/v1/payment-methods/{payment-method-id}/profiles', 'GET',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='GetStoredCredentialProfilesResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ost_cancel_stored_credential_profile(self, payment_method_id, profile_number, **kwargs):  # noqa: E501
        """Cancel a stored credential profile  # noqa: E501

        Cancels a stored credential profile within a payment method.  Cancelling the stored credential profile indicates that the stored credentials are no longer valid, per a customer request. You cannot reactivate the stored credential profile after you have cancelled it.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_cancel_stored_credential_profile(payment_method_id, profile_number, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str payment_method_id: ID of a payment method.  (required)
        :param int profile_number: Number that identifies a stored credential profile within the payment method.  (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: ModifiedStoredCredentialProfileResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ost_cancel_stored_credential_profile_with_http_info(payment_method_id, profile_number, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ost_cancel_stored_credential_profile_with_http_info(payment_method_id, profile_number, **kwargs)  # noqa: E501
            return data

    def p_ost_cancel_stored_credential_profile_with_http_info(self, payment_method_id, profile_number, **kwargs):  # noqa: E501
        """Cancel a stored credential profile  # noqa: E501

        Cancels a stored credential profile within a payment method.  Cancelling the stored credential profile indicates that the stored credentials are no longer valid, per a customer request. You cannot reactivate the stored credential profile after you have cancelled it.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_cancel_stored_credential_profile_with_http_info(payment_method_id, profile_number, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str payment_method_id: ID of a payment method.  (required)
        :param int profile_number: Number that identifies a stored credential profile within the payment method.  (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: ModifiedStoredCredentialProfileResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['payment_method_id', 'profile_number', 'idempotency_key', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ost_cancel_stored_credential_profile" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'payment_method_id' is set
        if self.api_client.client_side_validation and ('payment_method_id' not in params or
                                                       params['payment_method_id'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `payment_method_id` when calling `p_ost_cancel_stored_credential_profile`")  # noqa: E501
        # verify the required parameter 'profile_number' is set
        if self.api_client.client_side_validation and ('profile_number' not in params or
                                                       params['profile_number'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `profile_number` when calling `p_ost_cancel_stored_credential_profile`")  # noqa: E501

        if self.api_client.client_side_validation and ('idempotency_key' in params and
                                                       len(params['idempotency_key']) > 255):
            raise ValueError("Invalid value for parameter `idempotency_key` when calling `p_ost_cancel_stored_credential_profile`, length must be less than or equal to `255`")  # noqa: E501
        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ost_cancel_stored_credential_profile`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'payment_method_id' in params:
            path_params['payment-method-id'] = params['payment_method_id']  # noqa: E501
        if 'profile_number' in params:
            path_params['profile-number'] = params['profile_number']  # noqa: E501

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
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/v1/payment-methods/{payment-method-id}/profiles/{profile-number}/cancel', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='ModifiedStoredCredentialProfileResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ost_create_payment_session(self, request, **kwargs):  # noqa: E501
        """Create a payment session  # noqa: E501

        When implementing Apple Pay integration by using Zuora's JavaScript SDK, use this operation to create a payment session on your server side. The response of this API operation contains a token for the payment session data. Send the token back to your client side to use in the subsequent implementation step. For more information, see [Set up Apple Pay through the JavaScript SDK approach](https://knowledgecenter.zuora.com/Zuora_Payments/Payment_Methods/B_Define_Payment_Methods/Set_up_Apple_Pay_for_gateway_integrations_other_than_Adyen_Integration_v2.0).   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_create_payment_session(request, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param POSTCreatePaymentSessionRequest request:  (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: POSTCreatePaymentSessionResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ost_create_payment_session_with_http_info(request, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ost_create_payment_session_with_http_info(request, **kwargs)  # noqa: E501
            return data

    def p_ost_create_payment_session_with_http_info(self, request, **kwargs):  # noqa: E501
        """Create a payment session  # noqa: E501

        When implementing Apple Pay integration by using Zuora's JavaScript SDK, use this operation to create a payment session on your server side. The response of this API operation contains a token for the payment session data. Send the token back to your client side to use in the subsequent implementation step. For more information, see [Set up Apple Pay through the JavaScript SDK approach](https://knowledgecenter.zuora.com/Zuora_Payments/Payment_Methods/B_Define_Payment_Methods/Set_up_Apple_Pay_for_gateway_integrations_other_than_Adyen_Integration_v2.0).   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_create_payment_session_with_http_info(request, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param POSTCreatePaymentSessionRequest request:  (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: POSTCreatePaymentSessionResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['request', 'idempotency_key', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ost_create_payment_session" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'request' is set
        if self.api_client.client_side_validation and ('request' not in params or
                                                       params['request'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `request` when calling `p_ost_create_payment_session`")  # noqa: E501

        if self.api_client.client_side_validation and ('idempotency_key' in params and
                                                       len(params['idempotency_key']) > 255):
            raise ValueError("Invalid value for parameter `idempotency_key` when calling `p_ost_create_payment_session`, length must be less than or equal to `255`")  # noqa: E501
        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ost_create_payment_session`, length must be less than or equal to `64`")  # noqa: E501
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
        if 'request' in params:
            body_params = params['request']
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/web-payments/sessions', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='POSTCreatePaymentSessionResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ost_create_stored_credential_profile(self, payment_method_id, request, **kwargs):  # noqa: E501
        """Create a stored credential profile  # noqa: E501

        Creates a stored credential profile within a payment method.  The stored credential profile represents a consent agreement that you have established with a customer. When you use the payment method in a transaction, Zuora may include information from the stored credential profile to inform the payment processor that the transaction is related to your pre-existing consent agreement with the customer.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_create_stored_credential_profile(payment_method_id, request, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str payment_method_id: ID of a payment method.  (required)
        :param CreateStoredCredentialProfileRequest request:  (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: ModifiedStoredCredentialProfileResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ost_create_stored_credential_profile_with_http_info(payment_method_id, request, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ost_create_stored_credential_profile_with_http_info(payment_method_id, request, **kwargs)  # noqa: E501
            return data

    def p_ost_create_stored_credential_profile_with_http_info(self, payment_method_id, request, **kwargs):  # noqa: E501
        """Create a stored credential profile  # noqa: E501

        Creates a stored credential profile within a payment method.  The stored credential profile represents a consent agreement that you have established with a customer. When you use the payment method in a transaction, Zuora may include information from the stored credential profile to inform the payment processor that the transaction is related to your pre-existing consent agreement with the customer.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_create_stored_credential_profile_with_http_info(payment_method_id, request, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str payment_method_id: ID of a payment method.  (required)
        :param CreateStoredCredentialProfileRequest request:  (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: ModifiedStoredCredentialProfileResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['payment_method_id', 'request', 'idempotency_key', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ost_create_stored_credential_profile" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'payment_method_id' is set
        if self.api_client.client_side_validation and ('payment_method_id' not in params or
                                                       params['payment_method_id'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `payment_method_id` when calling `p_ost_create_stored_credential_profile`")  # noqa: E501
        # verify the required parameter 'request' is set
        if self.api_client.client_side_validation and ('request' not in params or
                                                       params['request'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `request` when calling `p_ost_create_stored_credential_profile`")  # noqa: E501

        if self.api_client.client_side_validation and ('idempotency_key' in params and
                                                       len(params['idempotency_key']) > 255):
            raise ValueError("Invalid value for parameter `idempotency_key` when calling `p_ost_create_stored_credential_profile`, length must be less than or equal to `255`")  # noqa: E501
        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ost_create_stored_credential_profile`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'payment_method_id' in params:
            path_params['payment-method-id'] = params['payment_method_id']  # noqa: E501

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
        if 'request' in params:
            body_params = params['request']
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/v1/payment-methods/{payment-method-id}/profiles', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='ModifiedStoredCredentialProfileResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ost_expire_stored_credential_profile(self, payment_method_id, profile_number, **kwargs):  # noqa: E501
        """Expire a stored credential profile  # noqa: E501

        Expires a stored credential profile within a payment method.  Expiring the stored credential profile indicates that the stored credentials are no longer valid, per an expiration policy in the stored credential transaction framework. You cannot reactivate the stored credential profile after you have expired it.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_expire_stored_credential_profile(payment_method_id, profile_number, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str payment_method_id: ID of a payment method.  (required)
        :param int profile_number: Number that identifies a stored credential profile within the payment method.  (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: ModifiedStoredCredentialProfileResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ost_expire_stored_credential_profile_with_http_info(payment_method_id, profile_number, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ost_expire_stored_credential_profile_with_http_info(payment_method_id, profile_number, **kwargs)  # noqa: E501
            return data

    def p_ost_expire_stored_credential_profile_with_http_info(self, payment_method_id, profile_number, **kwargs):  # noqa: E501
        """Expire a stored credential profile  # noqa: E501

        Expires a stored credential profile within a payment method.  Expiring the stored credential profile indicates that the stored credentials are no longer valid, per an expiration policy in the stored credential transaction framework. You cannot reactivate the stored credential profile after you have expired it.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_expire_stored_credential_profile_with_http_info(payment_method_id, profile_number, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str payment_method_id: ID of a payment method.  (required)
        :param int profile_number: Number that identifies a stored credential profile within the payment method.  (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: ModifiedStoredCredentialProfileResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['payment_method_id', 'profile_number', 'idempotency_key', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ost_expire_stored_credential_profile" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'payment_method_id' is set
        if self.api_client.client_side_validation and ('payment_method_id' not in params or
                                                       params['payment_method_id'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `payment_method_id` when calling `p_ost_expire_stored_credential_profile`")  # noqa: E501
        # verify the required parameter 'profile_number' is set
        if self.api_client.client_side_validation and ('profile_number' not in params or
                                                       params['profile_number'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `profile_number` when calling `p_ost_expire_stored_credential_profile`")  # noqa: E501

        if self.api_client.client_side_validation and ('idempotency_key' in params and
                                                       len(params['idempotency_key']) > 255):
            raise ValueError("Invalid value for parameter `idempotency_key` when calling `p_ost_expire_stored_credential_profile`, length must be less than or equal to `255`")  # noqa: E501
        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ost_expire_stored_credential_profile`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'payment_method_id' in params:
            path_params['payment-method-id'] = params['payment_method_id']  # noqa: E501
        if 'profile_number' in params:
            path_params['profile-number'] = params['profile_number']  # noqa: E501

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
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/v1/payment-methods/{payment-method-id}/profiles/{profile-number}/expire', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='ModifiedStoredCredentialProfileResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ost_payment_methods(self, request, **kwargs):  # noqa: E501
        """Create a payment method  # noqa: E501

        You can use this operation to create either a payment method associated with a specific customer account, or an orphan payment method that is not associated with any customer account. The orphan payment method must be associated with a customer account within 10 days. Otherwise, it will be deleted.  This operation supports the payment methods listed below.          ### Credit Card The following request body fields are applicable to this payment method: * `cardHolderInfo` (`cardHolderName` required) * `cardNumber` (required) * `cardType` (required) * `expirationMonth` (required) * `expirationYear` (required) * `mitConsentAgreementRef` * `mitConsentAgreementSrc` * `mitNetworkTransactionId` * `mitProfileAction` * `mitProfileType` * `mitProfileAgreedOn` * `securityCode` * `checkDuplicated` * `mandateInfo`  ### Credit Card (CC) Reference Transaction The following request body fields are applicable to this payment method: * `tokenId` (required) * `secondTokenId` * `creditCardMaskNumber` * `mandateInfo`  ### ACH The following request body fields are applicable to this payment method: * `bankABACode` (required) * `bankAccountName` (required) * `bankAccountNumber` (required) * `bankAccountType` (required) * `bankName` (required) * `addressLine1` * `addressLine2` * `phone` * `city` * `country` * `state` * `zipCode` * `mandateInfo`  ### SEPA The following request body fields are applicable to this payment method: * `IBAN` (required) * `accountHolderInfo` (required) * `businessIdentificationCode` * `mandateInfo`  ### Betalingsservice (Direct Debit DK) The following request body fields are applicable to this payment method: * `accountNumber` (required) * `identityNumber` (required) * `bankCode` (required) * `accountHolderInfo` (required) * `mandateInfo`  ### Autogiro (Direct Debit SE) The following request body fields are applicable to this payment method:         * `accountNumber` (required) * `identityNumber` (required) * `branchCode` (required) * `accountHolderInfo` (required)  * `mandateInfo`  ### Bacs (Direct Debit UK) The following request body fields are applicable to this payment method:         * `accountNumber` (required) * `bankCode` (required) * `accountHolderInfo` (required) * `mandateInfo`  ### Becs (Direct Entry AU) The following request body fields are applicable to this payment method:         * `accountNumber` (required) * `branchCode` (required) * `accountHolderInfo` (required) * `mandateInfo`  ### Becsnz (Direct Debit NZ) The following request body fields are applicable to this payment method:         * `accountNumber` (required) * `bankCode` (required) * `branchCode` (required) * `accountHolderInfo` (required) * `mandateInfo`  ### PAD (Pre-Authorized Debit) The following request body fields are applicable to this payment method: * `accountNumber` (required) * `bankCode` (required) * `branchCode` (required) * `accountHolderInfo` (required) * `mandateInfo`  ### PayPal Express Checkout The following request body fields are specific to this payment method: * `BAID` (required) * `email` (required)  ### PayPal Native Express Checkout The following request body fields are specific to this payment method: * `BAID` (required) * `email` (optional)  ### PayPal Adaptive The following request body fields are specific to this payment method: * `preapprovalKey` (required) * `email` (required)  ### PayPal Commerce Platform The following request body fields are applicable to the creation of a payment method with the PayPal Commerce Platform gateway integration: * `BAID` (required) * `email` (required)   ### Apple Pay on Adyen Integration v2.0 See [Set up Adyen Apple Pay](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/L_Payment_Methods/Payment_Method_Types/Apple_Pay_on_Web/Set_up_Adyen_Apple_Pay) for details.  ### Google Pay on Adyen Integration v2.0 See [Set up Adyen Google Pay](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/L_Payment_Methods/Payment_Method_Types/Set_up_Adyen_Google_Pay) for details.  ### Google Pay on Chase Paymentech Orbital Gateway See [Set up Google Pay on Chase](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/L_Payment_Methods/Payment_Method_Types/Set_up_Google_Pay_on_Chase) for details.  ### Custom payment methods With the support of Universal Payment Connector (UPC) and Open Payment Method (OPM) services, you can create custom payment methods by using the fields defined in a definition file for this type of custom payment method. See [Set up custom payment gateways and payment methods](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/MB_Set_up_custom_payment_gateways_and_payment_methods) for details.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_payment_methods(request, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param POSTPaymentMethodRequest request:  (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: POSTPaymentMethodResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ost_payment_methods_with_http_info(request, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ost_payment_methods_with_http_info(request, **kwargs)  # noqa: E501
            return data

    def p_ost_payment_methods_with_http_info(self, request, **kwargs):  # noqa: E501
        """Create a payment method  # noqa: E501

        You can use this operation to create either a payment method associated with a specific customer account, or an orphan payment method that is not associated with any customer account. The orphan payment method must be associated with a customer account within 10 days. Otherwise, it will be deleted.  This operation supports the payment methods listed below.          ### Credit Card The following request body fields are applicable to this payment method: * `cardHolderInfo` (`cardHolderName` required) * `cardNumber` (required) * `cardType` (required) * `expirationMonth` (required) * `expirationYear` (required) * `mitConsentAgreementRef` * `mitConsentAgreementSrc` * `mitNetworkTransactionId` * `mitProfileAction` * `mitProfileType` * `mitProfileAgreedOn` * `securityCode` * `checkDuplicated` * `mandateInfo`  ### Credit Card (CC) Reference Transaction The following request body fields are applicable to this payment method: * `tokenId` (required) * `secondTokenId` * `creditCardMaskNumber` * `mandateInfo`  ### ACH The following request body fields are applicable to this payment method: * `bankABACode` (required) * `bankAccountName` (required) * `bankAccountNumber` (required) * `bankAccountType` (required) * `bankName` (required) * `addressLine1` * `addressLine2` * `phone` * `city` * `country` * `state` * `zipCode` * `mandateInfo`  ### SEPA The following request body fields are applicable to this payment method: * `IBAN` (required) * `accountHolderInfo` (required) * `businessIdentificationCode` * `mandateInfo`  ### Betalingsservice (Direct Debit DK) The following request body fields are applicable to this payment method: * `accountNumber` (required) * `identityNumber` (required) * `bankCode` (required) * `accountHolderInfo` (required) * `mandateInfo`  ### Autogiro (Direct Debit SE) The following request body fields are applicable to this payment method:         * `accountNumber` (required) * `identityNumber` (required) * `branchCode` (required) * `accountHolderInfo` (required)  * `mandateInfo`  ### Bacs (Direct Debit UK) The following request body fields are applicable to this payment method:         * `accountNumber` (required) * `bankCode` (required) * `accountHolderInfo` (required) * `mandateInfo`  ### Becs (Direct Entry AU) The following request body fields are applicable to this payment method:         * `accountNumber` (required) * `branchCode` (required) * `accountHolderInfo` (required) * `mandateInfo`  ### Becsnz (Direct Debit NZ) The following request body fields are applicable to this payment method:         * `accountNumber` (required) * `bankCode` (required) * `branchCode` (required) * `accountHolderInfo` (required) * `mandateInfo`  ### PAD (Pre-Authorized Debit) The following request body fields are applicable to this payment method: * `accountNumber` (required) * `bankCode` (required) * `branchCode` (required) * `accountHolderInfo` (required) * `mandateInfo`  ### PayPal Express Checkout The following request body fields are specific to this payment method: * `BAID` (required) * `email` (required)  ### PayPal Native Express Checkout The following request body fields are specific to this payment method: * `BAID` (required) * `email` (optional)  ### PayPal Adaptive The following request body fields are specific to this payment method: * `preapprovalKey` (required) * `email` (required)  ### PayPal Commerce Platform The following request body fields are applicable to the creation of a payment method with the PayPal Commerce Platform gateway integration: * `BAID` (required) * `email` (required)   ### Apple Pay on Adyen Integration v2.0 See [Set up Adyen Apple Pay](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/L_Payment_Methods/Payment_Method_Types/Apple_Pay_on_Web/Set_up_Adyen_Apple_Pay) for details.  ### Google Pay on Adyen Integration v2.0 See [Set up Adyen Google Pay](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/L_Payment_Methods/Payment_Method_Types/Set_up_Adyen_Google_Pay) for details.  ### Google Pay on Chase Paymentech Orbital Gateway See [Set up Google Pay on Chase](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/L_Payment_Methods/Payment_Method_Types/Set_up_Google_Pay_on_Chase) for details.  ### Custom payment methods With the support of Universal Payment Connector (UPC) and Open Payment Method (OPM) services, you can create custom payment methods by using the fields defined in a definition file for this type of custom payment method. See [Set up custom payment gateways and payment methods](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/MB_Set_up_custom_payment_gateways_and_payment_methods) for details.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_payment_methods_with_http_info(request, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param POSTPaymentMethodRequest request:  (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: POSTPaymentMethodResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['request', 'idempotency_key', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ost_payment_methods" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'request' is set
        if self.api_client.client_side_validation and ('request' not in params or
                                                       params['request'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `request` when calling `p_ost_payment_methods`")  # noqa: E501

        if self.api_client.client_side_validation and ('idempotency_key' in params and
                                                       len(params['idempotency_key']) > 255):
            raise ValueError("Invalid value for parameter `idempotency_key` when calling `p_ost_payment_methods`, length must be less than or equal to `255`")  # noqa: E501
        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ost_payment_methods`, length must be less than or equal to `64`")  # noqa: E501
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
        if 'request' in params:
            body_params = params['request']
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/v1/payment-methods', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='POSTPaymentMethodResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ost_payment_methods_decryption(self, request, **kwargs):  # noqa: E501
        """Create an Apple Pay payment method  # noqa: E501

        The decryption API endpoint can conditionally perform 4 tasks in one atomic call:   * Decrypt Apple Pay Payment token   * Create Credit Card Payment Method in Zuora with decrypted Apple Pay information   * Create a stored credential profile within the Apple Pay payment method   * Process Payment on a specified Invoice (optional)   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_payment_methods_decryption(request, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param POSTPaymentMethodDecryption request:  (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: POSTPaymentMethodResponseDecryption
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ost_payment_methods_decryption_with_http_info(request, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ost_payment_methods_decryption_with_http_info(request, **kwargs)  # noqa: E501
            return data

    def p_ost_payment_methods_decryption_with_http_info(self, request, **kwargs):  # noqa: E501
        """Create an Apple Pay payment method  # noqa: E501

        The decryption API endpoint can conditionally perform 4 tasks in one atomic call:   * Decrypt Apple Pay Payment token   * Create Credit Card Payment Method in Zuora with decrypted Apple Pay information   * Create a stored credential profile within the Apple Pay payment method   * Process Payment on a specified Invoice (optional)   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_payment_methods_decryption_with_http_info(request, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param POSTPaymentMethodDecryption request:  (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: POSTPaymentMethodResponseDecryption
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['request', 'idempotency_key', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ost_payment_methods_decryption" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'request' is set
        if self.api_client.client_side_validation and ('request' not in params or
                                                       params['request'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `request` when calling `p_ost_payment_methods_decryption`")  # noqa: E501

        if self.api_client.client_side_validation and ('idempotency_key' in params and
                                                       len(params['idempotency_key']) > 255):
            raise ValueError("Invalid value for parameter `idempotency_key` when calling `p_ost_payment_methods_decryption`, length must be less than or equal to `255`")  # noqa: E501
        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ost_payment_methods_decryption`, length must be less than or equal to `64`")  # noqa: E501
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
        if 'request' in params:
            body_params = params['request']
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/v1/payment-methods/decryption', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='POSTPaymentMethodResponseDecryption',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ut_payment_method(self, payment_method_id, request, **kwargs):  # noqa: E501
        """Update a payment method  # noqa: E501

        This operation allows you to update an existing payment method.   The following request body fields can be updated for any payment method types except for Credit Card Reference Transaction: * `authGateway` * `gatewayOptions` * `accountHolderInfo` * `ipAddress` * Custom fields  The following request body fields can be updated only for the Credit Card payment method: * `expirationMonth`  * `expirationYear` * `securityCode`  The following request body field can be updated for the Credit Card, Credit Card Reference Transaction, ACH, and Bank Transfer payment methods: * `mandateInfo`    # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ut_payment_method(payment_method_id, request, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str payment_method_id: Unique ID of the payment method to update. (required)
        :param PUTPaymentMethodRequest request:  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: PUTPaymentMethodResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ut_payment_method_with_http_info(payment_method_id, request, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ut_payment_method_with_http_info(payment_method_id, request, **kwargs)  # noqa: E501
            return data

    def p_ut_payment_method_with_http_info(self, payment_method_id, request, **kwargs):  # noqa: E501
        """Update a payment method  # noqa: E501

        This operation allows you to update an existing payment method.   The following request body fields can be updated for any payment method types except for Credit Card Reference Transaction: * `authGateway` * `gatewayOptions` * `accountHolderInfo` * `ipAddress` * Custom fields  The following request body fields can be updated only for the Credit Card payment method: * `expirationMonth`  * `expirationYear` * `securityCode`  The following request body field can be updated for the Credit Card, Credit Card Reference Transaction, ACH, and Bank Transfer payment methods: * `mandateInfo`    # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ut_payment_method_with_http_info(payment_method_id, request, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str payment_method_id: Unique ID of the payment method to update. (required)
        :param PUTPaymentMethodRequest request:  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: PUTPaymentMethodResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['payment_method_id', 'request', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ut_payment_method" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'payment_method_id' is set
        if self.api_client.client_side_validation and ('payment_method_id' not in params or
                                                       params['payment_method_id'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `payment_method_id` when calling `p_ut_payment_method`")  # noqa: E501
        # verify the required parameter 'request' is set
        if self.api_client.client_side_validation and ('request' not in params or
                                                       params['request'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `request` when calling `p_ut_payment_method`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ut_payment_method`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'payment_method_id' in params:
            path_params['payment-method-id'] = params['payment_method_id']  # noqa: E501

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
        if 'request' in params:
            body_params = params['request']
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/v1/payment-methods/{payment-method-id}', 'PUT',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='PUTPaymentMethodResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ut_scrub_payment_methods(self, payment_method_id, **kwargs):  # noqa: E501
        """Scrub a payment method  # noqa: E501

         This operation enables you to replace all sensitive data in a payment method, related payment method snapshot table, and four related log tables with dummy values that will be stored in Zuora databases.   This operation will scrub the sensitive data and soft-delete the specified payment method at the same time.   If you want to delete or anonymize personal data in Zuora, you must scrub the payment method before anonymizing the associated account and contact. See [Delete or anonymize personal data](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Responding_to_individual_requests_for_access%2C_correction%2C_and_deletion_of_data_under_applicable_privacy_laws#Edit_and_correct_personal_data) for more information.  **Note:** In order to use this operation, you must ensure that the **Scrub Sensitive Data of Specific Payment Method payments** permission is enabled in your user role. Contact your tenant administrator if you want to enable this permission. See [Scrub Payment Methods](https://knowledgecenter.zuora.com/CB_Billing/L_Payment_Methods/Scrub_Payment_Methods) for more information.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ut_scrub_payment_methods(payment_method_id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str payment_method_id: The ID of the payment method where you want to scrub the sensitive data.  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: CommonResponseType
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ut_scrub_payment_methods_with_http_info(payment_method_id, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ut_scrub_payment_methods_with_http_info(payment_method_id, **kwargs)  # noqa: E501
            return data

    def p_ut_scrub_payment_methods_with_http_info(self, payment_method_id, **kwargs):  # noqa: E501
        """Scrub a payment method  # noqa: E501

         This operation enables you to replace all sensitive data in a payment method, related payment method snapshot table, and four related log tables with dummy values that will be stored in Zuora databases.   This operation will scrub the sensitive data and soft-delete the specified payment method at the same time.   If you want to delete or anonymize personal data in Zuora, you must scrub the payment method before anonymizing the associated account and contact. See [Delete or anonymize personal data](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Responding_to_individual_requests_for_access%2C_correction%2C_and_deletion_of_data_under_applicable_privacy_laws#Edit_and_correct_personal_data) for more information.  **Note:** In order to use this operation, you must ensure that the **Scrub Sensitive Data of Specific Payment Method payments** permission is enabled in your user role. Contact your tenant administrator if you want to enable this permission. See [Scrub Payment Methods](https://knowledgecenter.zuora.com/CB_Billing/L_Payment_Methods/Scrub_Payment_Methods) for more information.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ut_scrub_payment_methods_with_http_info(payment_method_id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str payment_method_id: The ID of the payment method where you want to scrub the sensitive data.  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: CommonResponseType
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['payment_method_id', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ut_scrub_payment_methods" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'payment_method_id' is set
        if self.api_client.client_side_validation and ('payment_method_id' not in params or
                                                       params['payment_method_id'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `payment_method_id` when calling `p_ut_scrub_payment_methods`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ut_scrub_payment_methods`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'payment_method_id' in params:
            path_params['payment-method-id'] = params['payment_method_id']  # noqa: E501

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
            '/v1/payment-methods/{payment-method-id}/scrub', 'PUT',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='CommonResponseType',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ut_verify_payment_methods(self, payment_method_id, body, **kwargs):  # noqa: E501
        """Verify a payment method  # noqa: E501

        Sends an authorization request to the corresponding payment gateway to verify the payment method, even though no changes are made for the payment method. Supported payment methods are Credit Cards and Paypal.  Zuora now supports performing a standalone zero dollar verification or one dollar authorization for credit cards. It also supports a billing agreement status check on PayPal payment methods.  If a payment method is created by Hosted Payment Pages and is not assigned to any billing account, the payment method cannot be verified through this operation.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ut_verify_payment_methods(payment_method_id, body, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str payment_method_id: The ID of the payment method to be verified.  (required)
        :param PUTVerifyPaymentMethodType body:  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :return: PUTVerifyPaymentMethodResponseType
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ut_verify_payment_methods_with_http_info(payment_method_id, body, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ut_verify_payment_methods_with_http_info(payment_method_id, body, **kwargs)  # noqa: E501
            return data

    def p_ut_verify_payment_methods_with_http_info(self, payment_method_id, body, **kwargs):  # noqa: E501
        """Verify a payment method  # noqa: E501

        Sends an authorization request to the corresponding payment gateway to verify the payment method, even though no changes are made for the payment method. Supported payment methods are Credit Cards and Paypal.  Zuora now supports performing a standalone zero dollar verification or one dollar authorization for credit cards. It also supports a billing agreement status check on PayPal payment methods.  If a payment method is created by Hosted Payment Pages and is not assigned to any billing account, the payment method cannot be verified through this operation.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ut_verify_payment_methods_with_http_info(payment_method_id, body, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str payment_method_id: The ID of the payment method to be verified.  (required)
        :param PUTVerifyPaymentMethodType body:  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :return: PUTVerifyPaymentMethodResponseType
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['payment_method_id', 'body', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ut_verify_payment_methods" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'payment_method_id' is set
        if self.api_client.client_side_validation and ('payment_method_id' not in params or
                                                       params['payment_method_id'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `payment_method_id` when calling `p_ut_verify_payment_methods`")  # noqa: E501
        # verify the required parameter 'body' is set
        if self.api_client.client_side_validation and ('body' not in params or
                                                       params['body'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `body` when calling `p_ut_verify_payment_methods`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ut_verify_payment_methods`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'payment_method_id' in params:
            path_params['payment-method-id'] = params['payment_method_id']  # noqa: E501

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

        form_params = []
        local_var_files = {}

        body_params = None
        if 'body' in params:
            body_params = params['body']
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.select_header_content_type(  # noqa: E501
            ['application/json'])  # noqa: E501

        # Authentication setting
        auth_settings = []  # noqa: E501

        return self.api_client.call_api(
            '/v1/payment-methods/{payment-method-id}/verify', 'PUT',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='PUTVerifyPaymentMethodResponseType',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)
