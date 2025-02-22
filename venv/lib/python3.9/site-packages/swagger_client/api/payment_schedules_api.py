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


class PaymentSchedulesApi(object):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    Ref: https://github.com/swagger-api/swagger-codegen
    """

    def __init__(self, api_client=None):
        if api_client is None:
            api_client = ApiClient()
        self.api_client = api_client

    def g_et_payment_schedule(self, payment_schedule_key, **kwargs):  # noqa: E501
        """Retrieve a payment schedule  # noqa: E501

        Retrieves a payment schedule by payment schedule key.  **Note:** - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manage and access this feature through the self-service interface, see [Manage Features](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features) in the Knowledge Center. - This operation is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_payment_schedule(payment_schedule_key, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str payment_schedule_key: The unique ID or number of a payment schedule. For example, `8a90857b822459cd018224dcb9eb13be`, or `PS-00000007`.  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param int next_pending_items: Number of next pending payment schedule items displayed in the response body. 
        :param int last_processed_items: Number of the most recent processed payment schedule items dispalyed in the response body. 
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the following response fields:   - `paymentOption`   - `items` > `paymentOption`   - `items` > `paymentId` 
        :return: GETPaymentScheduleResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.g_et_payment_schedule_with_http_info(payment_schedule_key, **kwargs)  # noqa: E501
        else:
            (data) = self.g_et_payment_schedule_with_http_info(payment_schedule_key, **kwargs)  # noqa: E501
            return data

    def g_et_payment_schedule_with_http_info(self, payment_schedule_key, **kwargs):  # noqa: E501
        """Retrieve a payment schedule  # noqa: E501

        Retrieves a payment schedule by payment schedule key.  **Note:** - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manage and access this feature through the self-service interface, see [Manage Features](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features) in the Knowledge Center. - This operation is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_payment_schedule_with_http_info(payment_schedule_key, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str payment_schedule_key: The unique ID or number of a payment schedule. For example, `8a90857b822459cd018224dcb9eb13be`, or `PS-00000007`.  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param int next_pending_items: Number of next pending payment schedule items displayed in the response body. 
        :param int last_processed_items: Number of the most recent processed payment schedule items dispalyed in the response body. 
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the following response fields:   - `paymentOption`   - `items` > `paymentOption`   - `items` > `paymentId` 
        :return: GETPaymentScheduleResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['payment_schedule_key', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids', 'next_pending_items', 'last_processed_items', 'zuora_version']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method g_et_payment_schedule" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'payment_schedule_key' is set
        if self.api_client.client_side_validation and ('payment_schedule_key' not in params or
                                                       params['payment_schedule_key'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `payment_schedule_key` when calling `g_et_payment_schedule`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `g_et_payment_schedule`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'payment_schedule_key' in params:
            path_params['paymentScheduleKey'] = params['payment_schedule_key']  # noqa: E501

        query_params = []
        if 'next_pending_items' in params:
            query_params.append(('nextPendingItems', params['next_pending_items']))  # noqa: E501
        if 'last_processed_items' in params:
            query_params.append(('lastProcessedItems', params['last_processed_items']))  # noqa: E501

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
        if 'zuora_version' in params:
            header_params['zuora-version'] = params['zuora_version']  # noqa: E501

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
            '/v1/payment-schedules/{paymentScheduleKey}', 'GET',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='GETPaymentScheduleResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def g_et_payment_schedule_item(self, item_id, **kwargs):  # noqa: E501
        """Retrieve a payment schedule item  # noqa: E501

        Retrieves a payment schedule item by ID.  **Note:** - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manage and access this feature through the self-service interface, see [Manage Features](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features) in the Knowledge Center. - This operation is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_payment_schedule_item(item_id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str item_id: The unique ID of a payment schedule item.  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the following response fields: - `paymentOption` - `paymentId` 
        :return: GETPaymentScheduleItemResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.g_et_payment_schedule_item_with_http_info(item_id, **kwargs)  # noqa: E501
        else:
            (data) = self.g_et_payment_schedule_item_with_http_info(item_id, **kwargs)  # noqa: E501
            return data

    def g_et_payment_schedule_item_with_http_info(self, item_id, **kwargs):  # noqa: E501
        """Retrieve a payment schedule item  # noqa: E501

        Retrieves a payment schedule item by ID.  **Note:** - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manage and access this feature through the self-service interface, see [Manage Features](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features) in the Knowledge Center. - This operation is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_payment_schedule_item_with_http_info(item_id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str item_id: The unique ID of a payment schedule item.  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the following response fields: - `paymentOption` - `paymentId` 
        :return: GETPaymentScheduleItemResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['item_id', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids', 'zuora_version']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method g_et_payment_schedule_item" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'item_id' is set
        if self.api_client.client_side_validation and ('item_id' not in params or
                                                       params['item_id'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `item_id` when calling `g_et_payment_schedule_item`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `g_et_payment_schedule_item`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'item_id' in params:
            path_params['item-id'] = params['item_id']  # noqa: E501

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
        if 'zuora_version' in params:
            header_params['zuora-version'] = params['zuora_version']  # noqa: E501

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
            '/v1/payment-schedule-items/{item-id}', 'GET',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='GETPaymentScheduleItemResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def g_et_payment_schedule_statistic(self, yyyy_mm_dd, **kwargs):  # noqa: E501
        """Retrieve payment schedule statistic of a date  # noqa: E501

        Retrieves the payment schedule statistic of a specific date.  **Note:** - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manage and access this feature through the self-service interface, see [Manage Features](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features) in the Knowledge Center.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_payment_schedule_statistic(yyyy_mm_dd, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param date yyyy_mm_dd: Specifies the date of the payment schedule statistic you want to view.   (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: GETPaymentScheduleStatisticResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.g_et_payment_schedule_statistic_with_http_info(yyyy_mm_dd, **kwargs)  # noqa: E501
        else:
            (data) = self.g_et_payment_schedule_statistic_with_http_info(yyyy_mm_dd, **kwargs)  # noqa: E501
            return data

    def g_et_payment_schedule_statistic_with_http_info(self, yyyy_mm_dd, **kwargs):  # noqa: E501
        """Retrieve payment schedule statistic of a date  # noqa: E501

        Retrieves the payment schedule statistic of a specific date.  **Note:** - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manage and access this feature through the self-service interface, see [Manage Features](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features) in the Knowledge Center.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_payment_schedule_statistic_with_http_info(yyyy_mm_dd, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param date yyyy_mm_dd: Specifies the date of the payment schedule statistic you want to view.   (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :return: GETPaymentScheduleStatisticResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['yyyy_mm_dd', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method g_et_payment_schedule_statistic" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'yyyy_mm_dd' is set
        if self.api_client.client_side_validation and ('yyyy_mm_dd' not in params or
                                                       params['yyyy_mm_dd'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `yyyy_mm_dd` when calling `g_et_payment_schedule_statistic`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `g_et_payment_schedule_statistic`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'yyyy_mm_dd' in params:
            path_params['yyyy-mm-dd'] = params['yyyy_mm_dd']  # noqa: E501

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
            '/v1/payment-schedules/statistics/{yyyy-mm-dd}', 'GET',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='GETPaymentScheduleStatisticResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def g_et_payment_schedules(self, **kwargs):  # noqa: E501
        """List payment schedules by customer account  # noqa: E501

        Retrieves payment schedules of a customer account.  **Note:** - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manage and access this feature through the self-service interface, see [Manage Features](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features) in the Knowledge Center. - This operation is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_payment_schedules(async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param int last_processed_items: Number of the most recent processed payment schedules dispalyed in the response body. 
        :param int next_pending_items: Number of next pending payment schedule items displayed in the response body. 
        :param str account_id: The ID of the customer account. If neither `accountId` nor `accountNumber` is specified, all payment schedules will be returned.  
        :param int account_number: The number of the customer account. If neither `accountId` nor `accountNumber` is specified, all payment schedules will be returned.  
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the following fields: * `paymentOption` * `items` > `paymentId`           
        :return: GETPaymentSchedulesResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.g_et_payment_schedules_with_http_info(**kwargs)  # noqa: E501
        else:
            (data) = self.g_et_payment_schedules_with_http_info(**kwargs)  # noqa: E501
            return data

    def g_et_payment_schedules_with_http_info(self, **kwargs):  # noqa: E501
        """List payment schedules by customer account  # noqa: E501

        Retrieves payment schedules of a customer account.  **Note:** - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manage and access this feature through the self-service interface, see [Manage Features](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features) in the Knowledge Center. - This operation is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.g_et_payment_schedules_with_http_info(async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param int last_processed_items: Number of the most recent processed payment schedules dispalyed in the response body. 
        :param int next_pending_items: Number of next pending payment schedule items displayed in the response body. 
        :param str account_id: The ID of the customer account. If neither `accountId` nor `accountNumber` is specified, all payment schedules will be returned.  
        :param int account_number: The number of the customer account. If neither `accountId` nor `accountNumber` is specified, all payment schedules will be returned.  
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the following fields: * `paymentOption` * `items` > `paymentId`           
        :return: GETPaymentSchedulesResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids', 'last_processed_items', 'next_pending_items', 'account_id', 'account_number', 'zuora_version']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method g_et_payment_schedules" % key
                )
            params[key] = val
        del params['kwargs']

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `g_et_payment_schedules`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}

        query_params = []
        if 'last_processed_items' in params:
            query_params.append(('lastProcessedItems', params['last_processed_items']))  # noqa: E501
        if 'next_pending_items' in params:
            query_params.append(('nextPendingItems', params['next_pending_items']))  # noqa: E501
        if 'account_id' in params:
            query_params.append(('accountId', params['account_id']))  # noqa: E501
        if 'account_number' in params:
            query_params.append(('accountNumber', params['account_number']))  # noqa: E501

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
        if 'zuora_version' in params:
            header_params['zuora-version'] = params['zuora_version']  # noqa: E501

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
            '/v1/payment-schedules', 'GET',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='GETPaymentSchedulesResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ost_add_items_to_custom_payment_schedule(self, body, payment_schedule_key, **kwargs):  # noqa: E501
        """Add payment schedule items to a custom payment schedule  # noqa: E501

        Adds payment schedule items to a custom payment schedule. You cannot use this operation to add payment schedule items to recurring payment schedules.  **Note:** - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manage and access this feature through the self-service interface, see [Manage Features](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features) in the Knowledge Center. - This operation is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_add_items_to_custom_payment_schedule(body, payment_schedule_key, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param POSTAddItemsToPaymentScheduleRequest body:  (required)
        :param str payment_schedule_key: The unique ID or number of a payment schedule. For example, `8a90857b822459cd018224dcb9eb13be`, or `PS-00000007`.  (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the following fields: - `items` > `paymentOption`  - `items` > `paymentId` 
        :return: GETPaymentScheduleResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ost_add_items_to_custom_payment_schedule_with_http_info(body, payment_schedule_key, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ost_add_items_to_custom_payment_schedule_with_http_info(body, payment_schedule_key, **kwargs)  # noqa: E501
            return data

    def p_ost_add_items_to_custom_payment_schedule_with_http_info(self, body, payment_schedule_key, **kwargs):  # noqa: E501
        """Add payment schedule items to a custom payment schedule  # noqa: E501

        Adds payment schedule items to a custom payment schedule. You cannot use this operation to add payment schedule items to recurring payment schedules.  **Note:** - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manage and access this feature through the self-service interface, see [Manage Features](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features) in the Knowledge Center. - This operation is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_add_items_to_custom_payment_schedule_with_http_info(body, payment_schedule_key, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param POSTAddItemsToPaymentScheduleRequest body:  (required)
        :param str payment_schedule_key: The unique ID or number of a payment schedule. For example, `8a90857b822459cd018224dcb9eb13be`, or `PS-00000007`.  (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the following fields: - `items` > `paymentOption`  - `items` > `paymentId` 
        :return: GETPaymentScheduleResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['body', 'payment_schedule_key', 'idempotency_key', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids', 'zuora_version']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ost_add_items_to_custom_payment_schedule" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'body' is set
        if self.api_client.client_side_validation and ('body' not in params or
                                                       params['body'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `body` when calling `p_ost_add_items_to_custom_payment_schedule`")  # noqa: E501
        # verify the required parameter 'payment_schedule_key' is set
        if self.api_client.client_side_validation and ('payment_schedule_key' not in params or
                                                       params['payment_schedule_key'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `payment_schedule_key` when calling `p_ost_add_items_to_custom_payment_schedule`")  # noqa: E501

        if self.api_client.client_side_validation and ('idempotency_key' in params and
                                                       len(params['idempotency_key']) > 255):
            raise ValueError("Invalid value for parameter `idempotency_key` when calling `p_ost_add_items_to_custom_payment_schedule`, length must be less than or equal to `255`")  # noqa: E501
        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ost_add_items_to_custom_payment_schedule`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'payment_schedule_key' in params:
            path_params['paymentScheduleKey'] = params['payment_schedule_key']  # noqa: E501

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
        if 'zuora_version' in params:
            header_params['zuora-version'] = params['zuora_version']  # noqa: E501

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
            '/v1/payment-schedules/{paymentScheduleKey}/items', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='GETPaymentScheduleResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ost_payment_schedule(self, body, **kwargs):  # noqa: E501
        """Create a payment schedule  # noqa: E501

        Creates a payment schedule. You can create either recurring payment schedules or custom payment schedules.  **Note:** - This operation is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled. - You can choose to use payment schedules to process payments associated with billing documents or unapplied payments. If Standalone Payment is enabled, you can also use payment schedules to process standalone payments. - This operation is version controlled. If you set `zuora-version` to `329.0`, when creating custom payment schedules associated with billing documents, you need to specify the billing document for each payment schedule item; If `zuora-version` is set to `330.0`, when creating custom payment schedules associated with billing documents, you only need to specify the billing documents at the payment schedule level. The default version number is `329.0`. However, we recommend that you specify the version to `330.0`. `329.0` will be deprecated soon.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_payment_schedule(body, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param POSTPaymentScheduleRequest body:  (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the following request fields: * `billingDocument` * `items` > `billingDocument` * `paymentOption` * `items` > `paymentOption` 
        :return: POSTPaymentScheduleResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ost_payment_schedule_with_http_info(body, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ost_payment_schedule_with_http_info(body, **kwargs)  # noqa: E501
            return data

    def p_ost_payment_schedule_with_http_info(self, body, **kwargs):  # noqa: E501
        """Create a payment schedule  # noqa: E501

        Creates a payment schedule. You can create either recurring payment schedules or custom payment schedules.  **Note:** - This operation is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled. - You can choose to use payment schedules to process payments associated with billing documents or unapplied payments. If Standalone Payment is enabled, you can also use payment schedules to process standalone payments. - This operation is version controlled. If you set `zuora-version` to `329.0`, when creating custom payment schedules associated with billing documents, you need to specify the billing document for each payment schedule item; If `zuora-version` is set to `330.0`, when creating custom payment schedules associated with billing documents, you only need to specify the billing documents at the payment schedule level. The default version number is `329.0`. However, we recommend that you specify the version to `330.0`. `329.0` will be deprecated soon.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_payment_schedule_with_http_info(body, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param POSTPaymentScheduleRequest body:  (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the following request fields: * `billingDocument` * `items` > `billingDocument` * `paymentOption` * `items` > `paymentOption` 
        :return: POSTPaymentScheduleResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['body', 'idempotency_key', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids', 'zuora_version']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ost_payment_schedule" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'body' is set
        if self.api_client.client_side_validation and ('body' not in params or
                                                       params['body'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `body` when calling `p_ost_payment_schedule`")  # noqa: E501

        if self.api_client.client_side_validation and ('idempotency_key' in params and
                                                       len(params['idempotency_key']) > 255):
            raise ValueError("Invalid value for parameter `idempotency_key` when calling `p_ost_payment_schedule`, length must be less than or equal to `255`")  # noqa: E501
        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ost_payment_schedule`, length must be less than or equal to `64`")  # noqa: E501
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
        if 'zuora_version' in params:
            header_params['zuora-version'] = params['zuora_version']  # noqa: E501

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
            '/v1/payment-schedules', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='POSTPaymentScheduleResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ost_payment_schedules(self, body, **kwargs):  # noqa: E501
        """Create multiple payment schedules at once  # noqa: E501

        Creates multiple payment schedules at once. You can create both recurring payment schedules and custom payment schedules in one request. The maximum number of payment schedules that can be created by a single request is 50. The maximum number of payment schedule items that each payment schedule can contain is 1000, i.e., you must specify less than 1000 items for a custom payment schedule, and the `occurrences` field must be less than 1000 for a recurring payment schedule.  **Note:** - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manage and access this feature through the self-service interface, see [Manage Features](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features) in the Knowledge Center. - This operation is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled. - If Standalone Payment is enabled, you can choose to use payment schedules to process payments associated with billing documents, standalone payments, or unapplied payments. If Standalone Payment is not enabled, you can only use payment schedules to process unapplied payments or payments associated with billing documents. - This operation is version controlled. If you set `zuora-version` to `329.0`, when creating custom payment schedules associated with billing documents, you need to specify the billing document for each payment schedule item; If `zuora-version` is set to `330.0`, when creating custom payment schedules associated with billing documents, you only need to specify the billing documents at the payment schedule level. The default version number is `329.0`. However, we recommend that you specify the version to `330.0`. `329.0` will be deprecated soon.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_payment_schedules(body, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param POSTPaymentSchedulesRequest body:  (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the following request fields: * `paymentSchedules` > `billingDocument` * `paymentSchedules` > `items` > `billingDocument` * `paymentSchedules` > `paymentOption` * `paymentSchedules` > `items` > `paymentOption` 
        :return: POSTPaymentSchedulesResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ost_payment_schedules_with_http_info(body, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ost_payment_schedules_with_http_info(body, **kwargs)  # noqa: E501
            return data

    def p_ost_payment_schedules_with_http_info(self, body, **kwargs):  # noqa: E501
        """Create multiple payment schedules at once  # noqa: E501

        Creates multiple payment schedules at once. You can create both recurring payment schedules and custom payment schedules in one request. The maximum number of payment schedules that can be created by a single request is 50. The maximum number of payment schedule items that each payment schedule can contain is 1000, i.e., you must specify less than 1000 items for a custom payment schedule, and the `occurrences` field must be less than 1000 for a recurring payment schedule.  **Note:** - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manage and access this feature through the self-service interface, see [Manage Features](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features) in the Knowledge Center. - This operation is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled. - If Standalone Payment is enabled, you can choose to use payment schedules to process payments associated with billing documents, standalone payments, or unapplied payments. If Standalone Payment is not enabled, you can only use payment schedules to process unapplied payments or payments associated with billing documents. - This operation is version controlled. If you set `zuora-version` to `329.0`, when creating custom payment schedules associated with billing documents, you need to specify the billing document for each payment schedule item; If `zuora-version` is set to `330.0`, when creating custom payment schedules associated with billing documents, you only need to specify the billing documents at the payment schedule level. The default version number is `329.0`. However, we recommend that you specify the version to `330.0`. `329.0` will be deprecated soon.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_payment_schedules_with_http_info(body, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param POSTPaymentSchedulesRequest body:  (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the following request fields: * `paymentSchedules` > `billingDocument` * `paymentSchedules` > `items` > `billingDocument` * `paymentSchedules` > `paymentOption` * `paymentSchedules` > `items` > `paymentOption` 
        :return: POSTPaymentSchedulesResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['body', 'idempotency_key', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids', 'zuora_version']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ost_payment_schedules" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'body' is set
        if self.api_client.client_side_validation and ('body' not in params or
                                                       params['body'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `body` when calling `p_ost_payment_schedules`")  # noqa: E501

        if self.api_client.client_side_validation and ('idempotency_key' in params and
                                                       len(params['idempotency_key']) > 255):
            raise ValueError("Invalid value for parameter `idempotency_key` when calling `p_ost_payment_schedules`, length must be less than or equal to `255`")  # noqa: E501
        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ost_payment_schedules`, length must be less than or equal to `64`")  # noqa: E501
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
        if 'zuora_version' in params:
            header_params['zuora-version'] = params['zuora_version']  # noqa: E501

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
            '/v1/payment-schedules/batch', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='POSTPaymentSchedulesResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ost_retry_payment_schedule_item(self, body, **kwargs):  # noqa: E501
        """Retry failed payment schedule items  # noqa: E501

        Retries failed payment schedule items. The payment method and payment gateway of the failed payment can be updated to new values before the retry.  Note that you can retry a payment schedule item only when the payment schedule item is either in the `Error` or `Pending` status.  **Note:** - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manage and access this feature through the self-service interface, see [Manage Features](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features) in the Knowledge Center. - This operation is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_retry_payment_schedule_item(body, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param POSTRetryPaymentScheduleItemRequest body:  (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the following response fields: - `items` > `paymentOption` - `items` > `paymentId` 
        :return: POSTRetryPaymentScheduleItemResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ost_retry_payment_schedule_item_with_http_info(body, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ost_retry_payment_schedule_item_with_http_info(body, **kwargs)  # noqa: E501
            return data

    def p_ost_retry_payment_schedule_item_with_http_info(self, body, **kwargs):  # noqa: E501
        """Retry failed payment schedule items  # noqa: E501

        Retries failed payment schedule items. The payment method and payment gateway of the failed payment can be updated to new values before the retry.  Note that you can retry a payment schedule item only when the payment schedule item is either in the `Error` or `Pending` status.  **Note:** - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manage and access this feature through the self-service interface, see [Manage Features](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features) in the Knowledge Center. - This operation is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ost_retry_payment_schedule_item_with_http_info(body, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param POSTRetryPaymentScheduleItemRequest body:  (required)
        :param str idempotency_key: Specify a unique idempotency key if you want to perform an idempotent POST or PATCH request. Do not use this header in other request types.   With this header specified, the Zuora server can identify subsequent retries of the same request using this value, which prevents the same operation from being performed multiple times by accident.  
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the following response fields: - `items` > `paymentOption` - `items` > `paymentId` 
        :return: POSTRetryPaymentScheduleItemResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['body', 'idempotency_key', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids', 'zuora_version']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ost_retry_payment_schedule_item" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'body' is set
        if self.api_client.client_side_validation and ('body' not in params or
                                                       params['body'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `body` when calling `p_ost_retry_payment_schedule_item`")  # noqa: E501

        if self.api_client.client_side_validation and ('idempotency_key' in params and
                                                       len(params['idempotency_key']) > 255):
            raise ValueError("Invalid value for parameter `idempotency_key` when calling `p_ost_retry_payment_schedule_item`, length must be less than or equal to `255`")  # noqa: E501
        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ost_retry_payment_schedule_item`, length must be less than or equal to `64`")  # noqa: E501
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
        if 'zuora_version' in params:
            header_params['zuora-version'] = params['zuora_version']  # noqa: E501

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
            '/v1/payment-schedule-items/retry-payment', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='POSTRetryPaymentScheduleItemResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ut_cancel_payment_schedule(self, payment_schedule_key, body, **kwargs):  # noqa: E501
        """Cancel a payment schedule  # noqa: E501

        Cancels a payment schedule.  **Note:** - All pending payment schedule items will be canceled, effective from `cancelDate`.  - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manage and access this feature through the self-service interface, see [Manage Features](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features) in the Knowledge Center. - This operation is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ut_cancel_payment_schedule(payment_schedule_key, body, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str payment_schedule_key: The unique ID or number of a payment schedule. For example, `8a90857b822459cd018224dcb9eb13be`, or `PS-00000007`.  (required)
        :param PUTCancelPaymentScheduleRequest body:  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the followiing response fields:   - `paymentOption`   - `items` > `paymentOption`   - `items` > `paymentId` 
        :return: GETPaymentScheduleResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ut_cancel_payment_schedule_with_http_info(payment_schedule_key, body, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ut_cancel_payment_schedule_with_http_info(payment_schedule_key, body, **kwargs)  # noqa: E501
            return data

    def p_ut_cancel_payment_schedule_with_http_info(self, payment_schedule_key, body, **kwargs):  # noqa: E501
        """Cancel a payment schedule  # noqa: E501

        Cancels a payment schedule.  **Note:** - All pending payment schedule items will be canceled, effective from `cancelDate`.  - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manage and access this feature through the self-service interface, see [Manage Features](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features) in the Knowledge Center. - This operation is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ut_cancel_payment_schedule_with_http_info(payment_schedule_key, body, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str payment_schedule_key: The unique ID or number of a payment schedule. For example, `8a90857b822459cd018224dcb9eb13be`, or `PS-00000007`.  (required)
        :param PUTCancelPaymentScheduleRequest body:  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the followiing response fields:   - `paymentOption`   - `items` > `paymentOption`   - `items` > `paymentId` 
        :return: GETPaymentScheduleResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['payment_schedule_key', 'body', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids', 'zuora_version']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ut_cancel_payment_schedule" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'payment_schedule_key' is set
        if self.api_client.client_side_validation and ('payment_schedule_key' not in params or
                                                       params['payment_schedule_key'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `payment_schedule_key` when calling `p_ut_cancel_payment_schedule`")  # noqa: E501
        # verify the required parameter 'body' is set
        if self.api_client.client_side_validation and ('body' not in params or
                                                       params['body'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `body` when calling `p_ut_cancel_payment_schedule`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ut_cancel_payment_schedule`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'payment_schedule_key' in params:
            path_params['paymentScheduleKey'] = params['payment_schedule_key']  # noqa: E501

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
        if 'zuora_version' in params:
            header_params['zuora-version'] = params['zuora_version']  # noqa: E501

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
            '/v1/payment-schedules/{paymentScheduleKey}/cancel', 'PUT',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='GETPaymentScheduleResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ut_cancel_payment_schedule_item(self, item_id, **kwargs):  # noqa: E501
        """Cancel a payment schedule item  # noqa: E501

        Cancels a payment schedule item by ID.  **Note:** - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manage and access this feature through the self-service interface, see [Manage Features](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features) in the Knowledge Center. - This operation is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ut_cancel_payment_schedule_item(item_id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str item_id: The unique ID of a payment schedule item.       (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the following response fields: - `paymentOption` - `paymentId` 
        :return: PUTPaymentScheduleItemResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ut_cancel_payment_schedule_item_with_http_info(item_id, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ut_cancel_payment_schedule_item_with_http_info(item_id, **kwargs)  # noqa: E501
            return data

    def p_ut_cancel_payment_schedule_item_with_http_info(self, item_id, **kwargs):  # noqa: E501
        """Cancel a payment schedule item  # noqa: E501

        Cancels a payment schedule item by ID.  **Note:** - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manage and access this feature through the self-service interface, see [Manage Features](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features) in the Knowledge Center. - This operation is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ut_cancel_payment_schedule_item_with_http_info(item_id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str item_id: The unique ID of a payment schedule item.       (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the following response fields: - `paymentOption` - `paymentId` 
        :return: PUTPaymentScheduleItemResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['item_id', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids', 'zuora_version']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ut_cancel_payment_schedule_item" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'item_id' is set
        if self.api_client.client_side_validation and ('item_id' not in params or
                                                       params['item_id'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `item_id` when calling `p_ut_cancel_payment_schedule_item`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ut_cancel_payment_schedule_item`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'item_id' in params:
            path_params['item-id'] = params['item_id']  # noqa: E501

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
        if 'zuora_version' in params:
            header_params['zuora-version'] = params['zuora_version']  # noqa: E501

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
            '/v1/payment-schedule-items/{item-id}/cancel', 'PUT',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='PUTPaymentScheduleItemResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ut_payment_schedule(self, body, payment_schedule_key, **kwargs):  # noqa: E501
        """Update a payment schedule  # noqa: E501

        Updates a payment schedule. For custom payment schedules, only the custom fields on the payment schedules can be udpated. Use the [Update a payment schedule item](https://www.zuora.com/developer/api-references/api/operation/PUT_PaymentScheduleItem/) operation to update payment schedule items of custom payment schedule.  Note the following rules for the `periodStartDate`, `period`, and `occurrences`:  - If `periodStartDate` is specified, all pending payment schedule items will be rescheduled using this new `periodStartDate`. If `period` is changed, we recommend passing in `periodStartDate`. Otherwise, the system would use the original start date if there is no processed or canceled items, or the last processed or canceled '`scheduleDate` + 1 `period`' as the new `periodStartDate`. - If only `occurrences` is updated, the new item will start with the last processed or canceled '`scheduleDate` + 1 `period`'.  **Note:** - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manage and access this feature through the self-service interface, see [Manage Features](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features) in the Knowledge Center. - This operation is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ut_payment_schedule(body, payment_schedule_key, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param PUTPaymentScheduleRequest body:  (required)
        :param str payment_schedule_key: The unique ID or number of a payment schedule. For example, `8a90857b822459cd018224dcb9eb13be`, or `PS-00000007`.  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the following fields: - `paymentOption` - `items` > `paymentId` 
        :return: GETPaymentScheduleResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ut_payment_schedule_with_http_info(body, payment_schedule_key, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ut_payment_schedule_with_http_info(body, payment_schedule_key, **kwargs)  # noqa: E501
            return data

    def p_ut_payment_schedule_with_http_info(self, body, payment_schedule_key, **kwargs):  # noqa: E501
        """Update a payment schedule  # noqa: E501

        Updates a payment schedule. For custom payment schedules, only the custom fields on the payment schedules can be udpated. Use the [Update a payment schedule item](https://www.zuora.com/developer/api-references/api/operation/PUT_PaymentScheduleItem/) operation to update payment schedule items of custom payment schedule.  Note the following rules for the `periodStartDate`, `period`, and `occurrences`:  - If `periodStartDate` is specified, all pending payment schedule items will be rescheduled using this new `periodStartDate`. If `period` is changed, we recommend passing in `periodStartDate`. Otherwise, the system would use the original start date if there is no processed or canceled items, or the last processed or canceled '`scheduleDate` + 1 `period`' as the new `periodStartDate`. - If only `occurrences` is updated, the new item will start with the last processed or canceled '`scheduleDate` + 1 `period`'.  **Note:** - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manage and access this feature through the self-service interface, see [Manage Features](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features) in the Knowledge Center. - This operation is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ut_payment_schedule_with_http_info(body, payment_schedule_key, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param PUTPaymentScheduleRequest body:  (required)
        :param str payment_schedule_key: The unique ID or number of a payment schedule. For example, `8a90857b822459cd018224dcb9eb13be`, or `PS-00000007`.  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the following fields: - `paymentOption` - `items` > `paymentId` 
        :return: GETPaymentScheduleResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['body', 'payment_schedule_key', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids', 'zuora_version']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ut_payment_schedule" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'body' is set
        if self.api_client.client_side_validation and ('body' not in params or
                                                       params['body'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `body` when calling `p_ut_payment_schedule`")  # noqa: E501
        # verify the required parameter 'payment_schedule_key' is set
        if self.api_client.client_side_validation and ('payment_schedule_key' not in params or
                                                       params['payment_schedule_key'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `payment_schedule_key` when calling `p_ut_payment_schedule`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ut_payment_schedule`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'payment_schedule_key' in params:
            path_params['paymentScheduleKey'] = params['payment_schedule_key']  # noqa: E501

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
        if 'zuora_version' in params:
            header_params['zuora-version'] = params['zuora_version']  # noqa: E501

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
            '/v1/payment-schedules/{paymentScheduleKey}', 'PUT',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='GETPaymentScheduleResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ut_payment_schedule_item(self, item_id, body, **kwargs):  # noqa: E501
        """Update a payment schedule item  # noqa: E501

        Updates a payment schedule item by ID.  **Note:** - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manag](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features). - This operation is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled. - To link a single payment to the payment schedule item, specify the `paymentID` field in the request body and set `zuora-version` to equal to or smaller than `336.0`; To link or unlink multiple payments with the item, specify the `linkPayments` and `unlinkPayments` field in the request body.  - The maximum number of payments that are allowed to be linked to a payment schedule item is `10`. - When unlinking and linking payments with the payment schedule item in one request, Zuora will first unlink the linked payments, and then link new payments to the item.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ut_payment_schedule_item(item_id, body, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str item_id: The unique ID of a payment schedule item.  (required)
        :param PUTPaymentScheduleItemRequest body:  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the following request fields: * `paymentId` * `linkPayments` * `unlinkPayments` * `paymentOption`  This header affects the availability of the following response fields: * `paymentId`             
        :return: PUTPaymentScheduleItemResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ut_payment_schedule_item_with_http_info(item_id, body, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ut_payment_schedule_item_with_http_info(item_id, body, **kwargs)  # noqa: E501
            return data

    def p_ut_payment_schedule_item_with_http_info(self, item_id, body, **kwargs):  # noqa: E501
        """Update a payment schedule item  # noqa: E501

        Updates a payment schedule item by ID.  **Note:** - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manag](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features). - This operation is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled. - To link a single payment to the payment schedule item, specify the `paymentID` field in the request body and set `zuora-version` to equal to or smaller than `336.0`; To link or unlink multiple payments with the item, specify the `linkPayments` and `unlinkPayments` field in the request body.  - The maximum number of payments that are allowed to be linked to a payment schedule item is `10`. - When unlinking and linking payments with the payment schedule item in one request, Zuora will first unlink the linked payments, and then link new payments to the item.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ut_payment_schedule_item_with_http_info(item_id, body, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str item_id: The unique ID of a payment schedule item.  (required)
        :param PUTPaymentScheduleItemRequest body:  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the following request fields: * `paymentId` * `linkPayments` * `unlinkPayments` * `paymentOption`  This header affects the availability of the following response fields: * `paymentId`             
        :return: PUTPaymentScheduleItemResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['item_id', 'body', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids', 'zuora_version']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ut_payment_schedule_item" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'item_id' is set
        if self.api_client.client_side_validation and ('item_id' not in params or
                                                       params['item_id'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `item_id` when calling `p_ut_payment_schedule_item`")  # noqa: E501
        # verify the required parameter 'body' is set
        if self.api_client.client_side_validation and ('body' not in params or
                                                       params['body'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `body` when calling `p_ut_payment_schedule_item`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ut_payment_schedule_item`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'item_id' in params:
            path_params['item-id'] = params['item_id']  # noqa: E501

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
        if 'zuora_version' in params:
            header_params['zuora-version'] = params['zuora_version']  # noqa: E501

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
            '/v1/payment-schedule-items/{item-id}', 'PUT',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='PUTPaymentScheduleItemResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ut_payment_schedule_update_preview(self, body, payment_schedule_key, **kwargs):  # noqa: E501
        """Preview the result of payment schedule updates  # noqa: E501

        Preview the result of a payment schedule update. This operation only provides a preview. No changes will be made to the database. For custom payment schedules, only the custom fields on the payment schedules can be udpated. Use the [Update a payment schedule item](https://www.zuora.com/developer/api-references/api/tag/PUT_PaymentScheduleItem) operation to update payment schedule items of custom payment schedule.  Note the following rules for the `periodStartDate`, `period`, and `occurrences`:  - If `periodStartDate` is specified, all pending payment schedule items will be rescheduled using this new `periodStartDate`. If `period` is changed, we recommend passing in `periodStartDate`. Otherwise, the system would use the original start date if there is no processed or canceled items, or the last processed or canceled '`scheduleDate` + 1 `period`' as the new `periodStartDate`. - If only `occurrences` is updated, the new item will start with the last processed or canceled '`scheduleDate` + 1 `period`'.  **Note:** - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manage and access this feature through the self-service interface, see [Manage Features](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features) in the Knowledge Center.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ut_payment_schedule_update_preview(body, payment_schedule_key, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param PUTPreviewPaymentScheduleRequest body:  (required)
        :param str payment_schedule_key: The unique ID or number of a payment schedule. For example, `8a90857b822459cd018224dcb9eb13be`, or `PS-00000007`.  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the following fields: - `paymentOption` - `items` > `paymentId` 
        :return: GETPaymentScheduleResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ut_payment_schedule_update_preview_with_http_info(body, payment_schedule_key, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ut_payment_schedule_update_preview_with_http_info(body, payment_schedule_key, **kwargs)  # noqa: E501
            return data

    def p_ut_payment_schedule_update_preview_with_http_info(self, body, payment_schedule_key, **kwargs):  # noqa: E501
        """Preview the result of payment schedule updates  # noqa: E501

        Preview the result of a payment schedule update. This operation only provides a preview. No changes will be made to the database. For custom payment schedules, only the custom fields on the payment schedules can be udpated. Use the [Update a payment schedule item](https://www.zuora.com/developer/api-references/api/tag/PUT_PaymentScheduleItem) operation to update payment schedule items of custom payment schedule.  Note the following rules for the `periodStartDate`, `period`, and `occurrences`:  - If `periodStartDate` is specified, all pending payment schedule items will be rescheduled using this new `periodStartDate`. If `period` is changed, we recommend passing in `periodStartDate`. Otherwise, the system would use the original start date if there is no processed or canceled items, or the last processed or canceled '`scheduleDate` + 1 `period`' as the new `periodStartDate`. - If only `occurrences` is updated, the new item will start with the last processed or canceled '`scheduleDate` + 1 `period`'.  **Note:** - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manage and access this feature through the self-service interface, see [Manage Features](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features) in the Knowledge Center.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ut_payment_schedule_update_preview_with_http_info(body, payment_schedule_key, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param PUTPreviewPaymentScheduleRequest body:  (required)
        :param str payment_schedule_key: The unique ID or number of a payment schedule. For example, `8a90857b822459cd018224dcb9eb13be`, or `PS-00000007`.  (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the following fields: - `paymentOption` - `items` > `paymentId` 
        :return: GETPaymentScheduleResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['body', 'payment_schedule_key', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids', 'zuora_version']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ut_payment_schedule_update_preview" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'body' is set
        if self.api_client.client_side_validation and ('body' not in params or
                                                       params['body'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `body` when calling `p_ut_payment_schedule_update_preview`")  # noqa: E501
        # verify the required parameter 'payment_schedule_key' is set
        if self.api_client.client_side_validation and ('payment_schedule_key' not in params or
                                                       params['payment_schedule_key'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `payment_schedule_key` when calling `p_ut_payment_schedule_update_preview`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ut_payment_schedule_update_preview`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'payment_schedule_key' in params:
            path_params['paymentScheduleKey'] = params['payment_schedule_key']  # noqa: E501

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
        if 'zuora_version' in params:
            header_params['zuora-version'] = params['zuora_version']  # noqa: E501

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
            '/v1/payment-schedules/{paymentScheduleKey}/preview', 'PUT',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='GETPaymentScheduleResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)

    def p_ut_skip_payment_schedule_item(self, item_id, **kwargs):  # noqa: E501
        """Skip a payment schedule item  # noqa: E501

        Skips a payment schedule item by ID. The skipped payment schedule item will turn to the `canceled` status, and a new item will be scheduled on the next recurring date after the last existing scheduled date.  **Note:** - Only payment schedule items in recurring payment schedules can be skipped, and the item must be in the `pending` status. - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manage and access this feature through the self-service interface, see [Manage Features](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features) in the Knowledge Center. - This operation is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ut_skip_payment_schedule_item(item_id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str item_id: The unique ID of a payment schedule item.     (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the following response fields: - `paymentOption` - `paymentId` 
        :return: PUTSkipPaymentScheduleItemResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.p_ut_skip_payment_schedule_item_with_http_info(item_id, **kwargs)  # noqa: E501
        else:
            (data) = self.p_ut_skip_payment_schedule_item_with_http_info(item_id, **kwargs)  # noqa: E501
            return data

    def p_ut_skip_payment_schedule_item_with_http_info(self, item_id, **kwargs):  # noqa: E501
        """Skip a payment schedule item  # noqa: E501

        Skips a payment schedule item by ID. The skipped payment schedule item will turn to the `canceled` status, and a new item will be scheduled on the next recurring date after the last existing scheduled date.  **Note:** - Only payment schedule items in recurring payment schedules can be skipped, and the item must be in the `pending` status. - The Payment Schedules feature is in the **Early Adopter** phase. We are actively soliciting feedback from a small set of early adopters before releasing it as generally available. To manage and access this feature through the self-service interface, see [Manage Features](https://knowledgecenter.zuora.com/Zuora_Payments/Payments_Settings/Manage_Features) in the Knowledge Center. - This operation is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled.   # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.p_ut_skip_payment_schedule_item_with_http_info(item_id, async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str item_id: The unique ID of a payment schedule item.     (required)
        :param str accept_encoding: Include the `Accept-Encoding: gzip` header to compress responses as a gzipped file. It can significantly reduce the bandwidth required for a response.   If specified, Zuora automatically compresses responses that contain over 1000 bytes of data, and the response contains a `Content-Encoding` header with the compression algorithm so that your client can decompress it. 
        :param str content_encoding: Include the `Content-Encoding: gzip` header to compress a request. With this header specified, you should upload a gzipped file for the request payload instead of sending the JSON payload. 
        :param str authorization: The value is in the `Bearer {token}` format where {token} is a valid OAuth token generated by calling [Create an OAuth token](https://www.zuora.com/developer/api-references/api/operation/createToken). 
        :param str zuora_track_id: A custom identifier for tracing the API call. If you set a value for this header, Zuora returns the same value in the response headers. This header enables you to associate your system process identifiers with Zuora API calls, to assist with troubleshooting in the event of an issue.  The value of this field must use the US-ASCII character set and must not include any of the following characters: colon (`:`), semicolon (`;`), double quote (`\"`), and quote (`'`). 
        :param str zuora_entity_ids: An entity ID. If you have [Zuora Multi-entity](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/Multi-entity) enabled and the OAuth token is valid for more than one entity, you must use this header to specify which entity to perform the operation in. If the OAuth token is only valid for a single entity, or you do not have Zuora Multi-entity enabled, you do not need to set this header. 
        :param str zuora_version: The minor version of the Zuora REST API. See [Minor Version](https://www.zuora.com/developer/api-references/api/overview/#section/API-Versions/Minor-Version) for information about REST API version control.   This header affects the availability of the following response fields: - `paymentOption` - `paymentId` 
        :return: PUTSkipPaymentScheduleItemResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['item_id', 'accept_encoding', 'content_encoding', 'authorization', 'zuora_track_id', 'zuora_entity_ids', 'zuora_version']  # noqa: E501
        all_params.append('async_req')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in six.iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method p_ut_skip_payment_schedule_item" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'item_id' is set
        if self.api_client.client_side_validation and ('item_id' not in params or
                                                       params['item_id'] is None):  # noqa: E501
            raise ValueError("Missing the required parameter `item_id` when calling `p_ut_skip_payment_schedule_item`")  # noqa: E501

        if self.api_client.client_side_validation and ('zuora_track_id' in params and
                                                       len(params['zuora_track_id']) > 64):
            raise ValueError("Invalid value for parameter `zuora_track_id` when calling `p_ut_skip_payment_schedule_item`, length must be less than or equal to `64`")  # noqa: E501
        collection_formats = {}

        path_params = {}
        if 'item_id' in params:
            path_params['item-id'] = params['item_id']  # noqa: E501

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
        if 'zuora_version' in params:
            header_params['zuora-version'] = params['zuora_version']  # noqa: E501

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
            '/v1/payment-schedule-items/{item-id}/skip', 'PUT',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=form_params,
            files=local_var_files,
            response_type='PUTSkipPaymentScheduleItemResponse',  # noqa: E501
            auth_settings=auth_settings,
            async_req=params.get('async_req'),
            _return_http_data_only=params.get('_return_http_data_only'),
            _preload_content=params.get('_preload_content', True),
            _request_timeout=params.get('_request_timeout'),
            collection_formats=collection_formats)
