# coding: utf-8

"""
    API Reference

      # Introduction  Welcome to the REST API reference for the Zuora Billing, Payments, and Central Platform!  To learn about the common use cases of Zuora REST APIs, check out the [REST API Tutorials](https://www.zuora.com/developer/rest-api/api-guides/overview/).  In addition to Zuora API Reference, we also provide API references for other Zuora products:    * [Revenue API Reference](https://www.zuora.com/developer/api-references/revenue/overview/)   * [Collections API Reference](https://www.zuora.com/developer/api-references/collections/overview/)      The Zuora REST API provides a broad set of operations and resources that:    * Enable Web Storefront integration from your website.   * Support self-service subscriber sign-ups and account management.   * Process revenue schedules through custom revenue rule models.   * Enable manipulation of most objects in the Zuora Billing Object Model.  Want to share your opinion on how our API works for you? <a href=\"https://community.zuora.com/t5/Developers/API-Feedback-Form/gpm-p/21399\" target=\"_blank\">Tell us how you feel </a>about using our API and what we can do to make it better.  Some of our older APIs are no longer recommended but still available, not affecting any existing integration. To find related API documentation, see [Older API Reference](https://www.zuora.com/developer/api-references/older-api/overview/).   ## Access to the API  If you have a Zuora tenant, you can access the Zuora REST API via one of the following endpoints:  | Tenant              | Base URL for REST Endpoints | |-------------------------|-------------------------| |US Cloud 1 Production | https://rest.na.zuora.com  | |US Cloud 1 API Sandbox |  https://rest.sandbox.na.zuora.com | |US Cloud 2 Production | https://rest.zuora.com | |US Cloud 2 API Sandbox | https://rest.apisandbox.zuora.com| |US Central Sandbox | https://rest.test.zuora.com |   |US Performance Test | https://rest.pt1.zuora.com | |US Production Copy | Submit a request at <a href=\"http://support.zuora.com/\" target=\"_blank\">Zuora Global Support</a> to enable the Zuora REST API in your tenant and obtain the base URL for REST endpoints. See [REST endpoint base URL of Production Copy (Service) Environment for existing and new customers](https://community.zuora.com/t5/API/REST-endpoint-base-URL-of-Production-Copy-Service-Environment/td-p/29611) for more information. | |EU Production | https://rest.eu.zuora.com | |EU API Sandbox | https://rest.sandbox.eu.zuora.com | |EU Central Sandbox | https://rest.test.eu.zuora.com |  The Production endpoint provides access to your live user data. Sandbox tenants are a good place to test code without affecting real-world data. If you would like Zuora to provision a Sandbox tenant for you, contact your Zuora representative for assistance.   If you do not have a Zuora tenant, go to <a href=\"https://www.zuora.com/resource/zuora-test-drive\" target=\"_blank\">https://www.zuora.com/resource/zuora-test-drive</a> and sign up for a Production Test Drive tenant. The tenant comes with seed data, including a sample product catalog.   # Error Handling  If a request to Zuora Billing REST API with an endpoint starting with `/v1` (except [Actions](https://www.zuora.com/developer/api-references/api/tag/Actions) and CRUD operations) fails, the response will contain an eight-digit error code with a corresponding error message to indicate the details of the error.  The following code snippet is a sample error response that contains an error code and message pair:  ```  {    \"success\": false,    \"processId\": \"CBCFED6580B4E076\",    \"reasons\":  [      {       \"code\": 53100320,       \"message\": \"'termType' value should be one of: TERMED, EVERGREEN\"      }     ]  } ``` The `success` field indicates whether the API request has succeeded. The `processId` field is a Zuora internal ID that you can provide to Zuora Global Support for troubleshooting purposes.  The `reasons` field contains the actual error code and message pair. The error code begins with `5` or `6` means that you encountered a certain issue that is specific to a REST API resource in Zuora Billing, Payments, and Central Platform. For example, `53100320` indicates that an invalid value is specified for the `termType` field of the `subscription` object.  The error code beginning with `9` usually indicates that an authentication-related issue occurred, and it can also indicate other unexpected errors depending on different cases. For example, `90000011` indicates that an invalid credential is provided in the request header.   When troubleshooting the error, you can divide the error code into two components: REST API resource code and error category code. See the following Zuora error code sample:  <a href=\"https://www.zuora.com/developer/images/ZuoraErrorCode.jpeg\" target=\"_blank\"><img src=\"https://www.zuora.com/developer/images/ZuoraErrorCode.jpeg\" alt=\"Zuora Error Code Sample\"></a>   **Note:** Zuora determines resource codes based on the request payload. Therefore, if GET and DELETE requests that do not contain payloads fail, you will get `500000` as the resource code, which indicates an unknown object and an unknown field.  The error category code of these requests is valid and follows the rules described in the [Error Category Codes](https://www.zuora.com/developer/api-references/api/overview/#section/Error-Handling/Error-Category-Codes) section.  In such case, you can refer to the returned error message to troubleshoot.   ## REST API Resource Codes  The 6-digit resource code indicates the REST API resource, typically a field of a Zuora object, on which the issue occurs. In the preceding example, `531003` refers to the `termType` field of the `subscription` object.   The value range for all REST API resource codes is from `500000` to `679999`. See <a href=\"https://knowledgecenter.zuora.com/Central_Platform/API/AA_REST_API/Resource_Codes\" target=\"_blank\">Resource Codes</a> in the Knowledge Center for a full list of resource codes.  ## Error Category Codes  The 2-digit error category code identifies the type of error, for example, resource not found or missing required field.   The following table describes all error categories and the corresponding resolution:  | Code    | Error category              | Description    | Resolution    | |:--------|:--------|:--------|:--------| | 10      | Permission or access denied | The request cannot be processed because a certain tenant or user permission is missing. | Check the missing tenant or user permission in the response message and contact <a href=\"https://support.zuora.com\" target=\"_blank\">Zuora Global Support</a> for enablement. | | 11      | Authentication failed       | Authentication fails due to invalid API authentication credentials. | Ensure that a valid API credential is specified. | | 20      | Invalid format or value     | The request cannot be processed due to an invalid field format or value. | Check the invalid field in the error message, and ensure that the format and value of all fields you passed in are valid. | | 21      | Unknown field in request    | The request cannot be processed because an unknown field exists in the request body. | Check the unknown field name in the response message, and ensure that you do not include any unknown field in the request body. | | 22      | Missing required field      | The request cannot be processed because a required field in the request body is missing. | Check the missing field name in the response message, and ensure that you include all required fields in the request body. | | 23      | Missing required parameter  | The request cannot be processed because a required query parameter is missing. | Check the missing parameter name in the response message, and ensure that you include the parameter in the query. | | 30      | Rule restriction            | The request cannot be processed due to the violation of a Zuora business rule. | Check the response message and ensure that the API request meets the specified business rules. | | 40      | Not found                   | The specified resource cannot be found. | Check the response message and ensure that the specified resource exists in your Zuora tenant. | | 45      | Unsupported request         | The requested endpoint does not support the specified HTTP method. | Check your request and ensure that the endpoint and method matches. | | 50      | Locking contention          | This request cannot be processed because the objects this request is trying to modify are being modified by another API request, UI operation, or batch job process. | <p>Resubmit the request first to have another try.</p> <p>If this error still occurs, contact <a href=\"https://support.zuora.com\" target=\"_blank\">Zuora Global Support</a> with the returned `Zuora-Request-Id` value in the response header for assistance.</p> | | 60      | Internal error              | The server encounters an internal error. | Contact <a href=\"https://support.zuora.com\" target=\"_blank\">Zuora Global Support</a> with the returned `Zuora-Request-Id` value in the response header for assistance. | | 61      | Temporary error             | A temporary error occurs during request processing, for example, a database communication error. | <p>Resubmit the request first to have another try.</p> <p>If this error still occurs, contact <a href=\"https://support.zuora.com\" target=\"_blank\">Zuora Global Support</a> with the returned `Zuora-Request-Id` value in the response header for assistance. </p>| | 70      | Request exceeded limit      | The total number of concurrent requests exceeds the limit allowed by the system. | <p>Resubmit the request after the number of seconds specified by the `Retry-After` value in the response header.</p> <p>Check [Concurrent request limits](https://www.zuora.com/developer/rest-api/general-concepts/rate-concurrency-limits/) for details about Zuora’s concurrent request limit policy.</p> | | 90      | Malformed request           | The request cannot be processed due to JSON syntax errors. | Check the syntax error in the JSON request body and ensure that the request is in the correct JSON format. | | 99      | Integration error           | The server encounters an error when communicating with an external system, for example, payment gateway, tax engine provider. | Check the response message and take action accordingly. |   # API Versions  The Zuora REST API are version controlled. Versioning ensures that Zuora REST API changes are backward compatible. Zuora uses a major and minor version nomenclature to manage changes. By specifying a version in a REST request, you can get expected responses regardless of future changes to the API.  ## Major Version  The major version number of the REST API appears in the REST URL. In this API reference, only the **v1** major version is available. For example, `POST https://rest.zuora.com/v1/subscriptions`.  ## Minor Version  Zuora uses minor versions for the REST API to control small changes. For example, a field in a REST method is deprecated and a new field is used to replace it.   Some fields in the REST methods are supported as of minor versions. If a field is not noted with a minor version, this field is available for all minor versions. If a field is noted with a minor version, this field is in version control. You must specify the supported minor version in the request header to process without an error.   If a field is in version control, it is either with a minimum minor version or a maximum minor version, or both of them. You can only use this field with the minor version between the minimum and the maximum minor versions. For example, the `invoiceCollect` field in the POST Subscription method is in version control and its maximum minor version is 189.0. You can only use this field with the minor version 189.0 or earlier.  If you specify a version number in the request header that is not supported, Zuora will use the minimum minor version of the REST API. In our REST API documentation, if a field or feature requires a minor version number, we note that in the field description.  You only need to specify the version number when you use the fields require a minor version. To specify the minor version, set the `zuora-version` parameter to the minor version number in the request header for the request call. For example, the `collect` field is in 196.0 minor version. If you want to use this field for the POST Subscription method, set the  `zuora-version` parameter to `196.0` in the request header. The `zuora-version` parameter is case sensitive.  For all the REST API fields, by default, if the minor version is not specified in the request header, Zuora will use the minimum minor version of the REST API to avoid breaking your integration.   ### Minor Version History  The supported minor versions are not serial. This section documents the changes made to each Zuora REST API minor version.  The following table lists the supported versions and the fields that have a Zuora REST API minor version.  | Fields         | Minor Version      | REST Methods    | Description | |:--------|:--------|:--------|:--------| | invoiceCollect | 189.0 and earlier  | [Create Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_Subscription \"Create Subscription\"); [Update Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_Subscription \"Update Subscription\"); [Renew Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_RenewSubscription \"Renew Subscription\"); [Cancel Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_CancelSubscription \"Cancel Subscription\"); [Suspend Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_SuspendSubscription \"Suspend Subscription\"); [Resume Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_ResumeSubscription \"Resume Subscription\"); [Create Account](https://www.zuora.com/developer/api-references/api/operation/POST_Account \"Create Account\")|Generates an invoice and collects a payment for a subscription. | | collect        | 196.0 and later    | [Create Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_Subscription \"Create Subscription\"); [Update Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_Subscription \"Update Subscription\"); [Renew Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_RenewSubscription \"Renew Subscription\"); [Cancel Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_CancelSubscription \"Cancel Subscription\"); [Suspend Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_SuspendSubscription \"Suspend Subscription\"); [Resume Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_ResumeSubscription \"Resume Subscription\"); [Create Account](https://www.zuora.com/developer/api-references/api/operation/POST_Account \"Create Account\")|Collects an automatic payment for a subscription. | | invoice | 196.0 and 207.0| [Create Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_Subscription \"Create Subscription\"); [Update Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_Subscription \"Update Subscription\"); [Renew Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_RenewSubscription \"Renew Subscription\"); [Cancel Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_CancelSubscription \"Cancel Subscription\"); [Suspend Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_SuspendSubscription \"Suspend Subscription\"); [Resume Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_ResumeSubscription \"Resume Subscription\"); [Create Account](https://www.zuora.com/developer/api-references/api/operation/POST_Account \"Create Account\")|Generates an invoice for a subscription. | | invoiceTargetDate | 206.0 and earlier  | [Preview Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_PreviewSubscription \"Preview Subscription\") |Date through which charges are calculated on the invoice, as `yyyy-mm-dd`. | | invoiceTargetDate | 207.0 and earlier  | [Create Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_Subscription \"Create Subscription\"); [Update Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_Subscription \"Update Subscription\"); [Renew Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_RenewSubscription \"Renew Subscription\"); [Cancel Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_CancelSubscription \"Cancel Subscription\"); [Suspend Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_SuspendSubscription \"Suspend Subscription\"); [Resume Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_ResumeSubscription \"Resume Subscription\"); [Create Account](https://www.zuora.com/developer/api-references/api/operation/POST_Account \"Create Account\")|Date through which charges are calculated on the invoice, as `yyyy-mm-dd`. | | targetDate | 207.0 and later | [Preview Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_PreviewSubscription \"Preview Subscription\") |Date through which charges are calculated on the invoice, as `yyyy-mm-dd`. | | targetDate | 211.0 and later | [Create Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_Subscription \"Create Subscription\"); [Update Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_Subscription \"Update Subscription\"); [Renew Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_RenewSubscription \"Renew Subscription\"); [Cancel Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_CancelSubscription \"Cancel Subscription\"); [Suspend Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_SuspendSubscription \"Suspend Subscription\"); [Resume Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_ResumeSubscription \"Resume Subscription\"); [Create Account](https://www.zuora.com/developer/api-references/api/operation/POST_Account \"Create Account\")|Date through which charges are calculated on the invoice, as `yyyy-mm-dd`. | | includeExisting DraftInvoiceItems | 206.0 and earlier| [Preview Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_PreviewSubscription \"Preview Subscription\"); [Update Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_Subscription \"Update Subscription\") | Specifies whether to include draft invoice items in subscription previews. Specify it to be `true` (default) to include draft invoice items in the preview result. Specify it to be `false` to excludes draft invoice items in the preview result. | | includeExisting DraftDocItems | 207.0 and later  | [Preview Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_PreviewSubscription \"Preview Subscription\"); [Update Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_Subscription \"Update Subscription\") | Specifies whether to include draft invoice items in subscription previews. Specify it to be `true` (default) to include draft invoice items in the preview result. Specify it to be `false` to excludes draft invoice items in the preview result. | | previewType | 206.0 and earlier| [Preview Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_PreviewSubscription \"Preview Subscription\"); [Update Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_Subscription \"Update Subscription\") | The type of preview you will receive. The possible values are `InvoiceItem`(default), `ChargeMetrics`, and `InvoiceItemChargeMetrics`. | | previewType | 207.0 and later  | [Preview Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_PreviewSubscription \"Preview Subscription\"); [Update Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_Subscription \"Update Subscription\") | The type of preview you will receive. The possible values are `LegalDoc`(default), `ChargeMetrics`, and `LegalDocChargeMetrics`. | | runBilling  | 211.0 and later  | [Create Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_Subscription \"Create Subscription\"); [Update Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_Subscription \"Update Subscription\"); [Renew Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_RenewSubscription \"Renew Subscription\"); [Cancel Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_CancelSubscription \"Cancel Subscription\"); [Suspend Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_SuspendSubscription \"Suspend Subscription\"); [Resume Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_ResumeSubscription \"Resume Subscription\"); [Create Account](https://www.zuora.com/developer/api-references/api/operation/POST_Account \"Create Account\")|Generates an invoice or credit memo for a subscription. **Note:** Credit memos are only available if you have the Invoice Settlement feature enabled. | | invoiceDate | 214.0 and earlier  | [Invoice and Collect](https://www.zuora.com/developer/api-references/api/operation/POST_TransactionInvoicePayment \"Invoice and Collect\") |Date that should appear on the invoice being generated, as `yyyy-mm-dd`. | | invoiceTargetDate | 214.0 and earlier  | [Invoice and Collect](https://www.zuora.com/developer/api-references/api/operation/POST_TransactionInvoicePayment \"Invoice and Collect\") |Date through which to calculate charges on this account if an invoice is generated, as `yyyy-mm-dd`. | | documentDate | 215.0 and later | [Invoice and Collect](https://www.zuora.com/developer/api-references/api/operation/POST_TransactionInvoicePayment \"Invoice and Collect\") |Date that should appear on the invoice and credit memo being generated, as `yyyy-mm-dd`. | | targetDate | 215.0 and later | [Invoice and Collect](https://www.zuora.com/developer/api-references/api/operation/POST_TransactionInvoicePayment \"Invoice and Collect\") |Date through which to calculate charges on this account if an invoice or a credit memo is generated, as `yyyy-mm-dd`. | | memoItemAmount | 223.0 and earlier | [Create credit memo from charge](https://www.zuora.com/developer/api-references/api/operation/POST_CreditMemoFromPrpc \"Create credit memo from charge\"); [Create debit memo from charge](https://www.zuora.com/developer/api-references/api/operation/POST_DebitMemoFromPrpc \"Create debit memo from charge\") | Amount of the memo item. | | amount | 224.0 and later | [Create credit memo from charge](https://www.zuora.com/developer/api-references/api/operation/POST_CreditMemoFromPrpc \"Create credit memo from charge\"); [Create debit memo from charge](https://www.zuora.com/developer/api-references/api/operation/POST_DebitMemoFromPrpc \"Create debit memo from charge\") | Amount of the memo item. | | subscriptionNumbers | 222.4 and earlier | [Create order](https://www.zuora.com/developer/api-references/api/operation/POST_Order \"Create order\") | Container for the subscription numbers of the subscriptions in an order. | | subscriptions | 223.0 and later | [Create order](https://www.zuora.com/developer/api-references/api/operation/POST_Order \"Create order\") | Container for the subscription numbers and statuses in an order. | | creditTaxItems | 238.0 and earlier | [Get credit memo items](https://www.zuora.com/developer/api-references/api/operation/GET_CreditMemoItems \"Get credit memo items\"); [Get credit memo item](https://www.zuora.com/developer/api-references/api/operation/GET_CreditMemoItem \"Get credit memo item\") | Container for the taxation items of the credit memo item. | | taxItems | 238.0 and earlier | [Get debit memo items](https://www.zuora.com/developer/api-references/api/operation/GET_DebitMemoItems \"Get debit memo items\"); [Get debit memo item](https://www.zuora.com/developer/api-references/api/operation/GET_DebitMemoItem \"Get debit memo item\") | Container for the taxation items of the debit memo item. | | taxationItems | 239.0 and later | [Get credit memo items](https://www.zuora.com/developer/api-references/api/operation/GET_CreditMemoItems \"Get credit memo items\"); [Get credit memo item](https://www.zuora.com/developer/api-references/api/operation/GET_CreditMemoItem \"Get credit memo item\"); [Get debit memo items](https://www.zuora.com/developer/api-references/api/operation/GET_DebitMemoItems \"Get debit memo items\"); [Get debit memo item](https://www.zuora.com/developer/api-references/api/operation/GET_DebitMemoItem \"Get debit memo item\") | Container for the taxation items of the memo item. | | chargeId | 256.0 and earlier | [Create credit memo from charge](https://www.zuora.com/developer/api-references/api/operation/POST_CreditMemoFromPrpc \"Create credit memo from charge\"); [Create debit memo from charge](https://www.zuora.com/developer/api-references/api/operation/POST_DebitMemoFromPrpc \"Create debit memo from charge\") | ID of the product rate plan charge that the memo is created from. | | productRatePlanChargeId | 257.0 and later | [Create credit memo from charge](https://www.zuora.com/developer/api-references/api/operation/POST_CreditMemoFromPrpc \"Create credit memo from charge\"); [Create debit memo from charge](https://www.zuora.com/developer/api-references/api/operation/POST_DebitMemoFromPrpc \"Create debit memo from charge\") | ID of the product rate plan charge that the memo is created from. | | comment | 256.0 and earlier | [Create credit memo from charge](https://www.zuora.com/developer/api-references/api/operation/POST_CreditMemoFromPrpc \"Create credit memo from charge\"); [Create debit memo from charge](https://www.zuora.com/developer/api-references/api/operation/POST_DebitMemoFromPrpc \"Create debit memo from charge\"); [Create credit memo from invoice](https://www.zuora.com/developer/api-references/api/operation/POST_CreditMemoFromInvoice \"Create credit memo from invoice\"); [Create debit memo from invoice](https://www.zuora.com/developer/api-references/api/operation/POST_DebitMemoFromInvoice \"Create debit memo from invoice\"); [Get credit memo items](https://www.zuora.com/developer/api-references/api/operation/GET_CreditMemoItems \"Get credit memo items\"); [Get credit memo item](https://www.zuora.com/developer/api-references/api/operation/GET_CreditMemoItem \"Get credit memo item\"); [Get debit memo items](https://www.zuora.com/developer/api-references/api/operation/GET_DebitMemoItems \"Get debit memo items\"); [Get debit memo item](https://www.zuora.com/developer/api-references/api/operation/GET_DebitMemoItem \"Get debit memo item\") | Comments about the product rate plan charge, invoice item, or memo item. | | description | 257.0 and later | [Create credit memo from charge](https://www.zuora.com/developer/api-references/api/operation/POST_CreditMemoFromPrpc \"Create credit memo from charge\"); [Create debit memo from charge](https://www.zuora.com/developer/api-references/api/operation/POST_DebitMemoFromPrpc \"Create debit memo from charge\"); [Create credit memo from invoice](https://www.zuora.com/developer/api-references/api/operation/POST_CreditMemoFromInvoice \"Create credit memo from invoice\"); [Create debit memo from invoice](https://www.zuora.com/developer/api-references/api/operation/POST_DebitMemoFromInvoice \"Create debit memo from invoice\"); [Get credit memo items](https://www.zuora.com/developer/api-references/api/operation/GET_CreditMemoItems \"Get credit memo items\"); [Get credit memo item](https://www.zuora.com/developer/api-references/api/operation/GET_CreditMemoItem \"Get credit memo item\"); [Get debit memo items](https://www.zuora.com/developer/api-references/api/operation/GET_DebitMemoItems \"Get debit memo items\"); [Get debit memo item](https://www.zuora.com/developer/api-references/api/operation/GET_DebitMemoItem \"Get debit memo item\") | Description of the the product rate plan charge, invoice item, or memo item. | | taxationItems | 309.0 and later | [Preview an order](https://www.zuora.com/developer/api-references/api/operation/POST_PreviewOrder \"Preview an order\") | List of taxation items for an invoice item or a credit memo item. | | batch | 309.0 and earlier | [Create a billing preview run](https://www.zuora.com/developer/api-references/api/operation/POST_BillingPreviewRun \"Create a billing preview run\") | The customer batches to include in the billing preview run. |       | batches | 314.0 and later | [Create a billing preview run](https://www.zuora.com/developer/api-references/api/operation/POST_BillingPreviewRun \"Create a billing preview run\") | The customer batches to include in the billing preview run. | | taxationItems | 315.0 and later | [Preview a subscription](https://www.zuora.com/developer/api-references/api/operation/POST_PreviewSubscription \"Preview a subscription\"); [Update a subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_Subscription \"Update a subscription\")| List of taxation items for an invoice item or a credit memo item. | | billingDocument | 330.0 and later | [Create a payment schedule](https://www.zuora.com/developer/api-references/api/operation/POST_PaymentSchedule \"Create a payment schedule\"); [Create multiple payment schedules at once](https://www.zuora.com/developer/api-references/api/operation/POST_PaymentSchedules \"Create multiple payment schedules at once\")| The billing document with which the payment schedule item is associated. | | paymentId | 336.0 and earlier | [Add payment schedule items to a custom payment schedule](https://www.zuora.com/developer/api-references/api/operation/POST_AddItemsToCustomPaymentSchedule/ \"Add payment schedule items to a custom payment schedule\"); [Update a payment schedule](https://www.zuora.com/developer/api-references/api/operation/PUT_PaymentSchedule/ \"Update a payment schedule\"); [Update a payment schedule item](https://www.zuora.com/developer/api-references/api/operation/PUT_PaymentScheduleItem/ \"Update a payment schedule item\"); [Preview the result of payment schedule update](https://www.zuora.com/developer/api-references/api/operation/PUT_PaymentScheduleUpdatePreview/ \"Preview the result of payment schedule update\"); [Retrieve a payment schedule](https://www.zuora.com/developer/api-references/api/operation/GET_PaymentSchedule/ \"Retrieve a payment schedule\"); [Retrieve a payment schedule item](https://www.zuora.com/developer/api-references/api/operation/GET_PaymentScheduleItem/ \"Retrieve a payment schedule item\"); [List payment schedules by customer account](https://www.zuora.com/developer/api-references/api/operation/GET_PaymentSchedules/ \"List payment schedules by customer account\"); [Cancel a payment schedule](https://www.zuora.com/developer/api-references/api/operation/PUT_CancelPaymentSchedule/ \"Cancel a payment schedule\"); [Cancel a payment schedule item](https://www.zuora.com/developer/api-references/api/operation/PUT_CancelPaymentScheduleItem/ \"Cancel a payment schedule item\");[Skip a payment schedule item](https://www.zuora.com/developer/api-references/api/operation/PUT_SkipPaymentScheduleItem/ \"Skip a payment schedule item\");[Retry failed payment schedule items](https://www.zuora.com/developer/api-references/api/operation/POST_RetryPaymentScheduleItem/ \"Retry failed payment schedule items\") | ID of the payment to be linked to the payment schedule item. | | paymentOption | 337.0 and later | [Create a payment schedule](https://www.zuora.com/developer/api-references/api/operation/POST_PaymentSchedule/ \"Create a payment schedule\"); [Create multiple payment schedules at once](https://www.zuora.com/developer/api-references/api/operation/POST_PaymentSchedules/ \"Create multiple payment schedules at once\"); [Create a payment](https://www.zuora.com/developer/api-references/api/operation/POST_CreatePayment/ \"Create a payment\"); [Add payment schedule items to a custom payment schedule](https://www.zuora.com/developer/api-references/api/operation/POST_AddItemsToCustomPaymentSchedule/ \"Add payment schedule items to a custom payment schedule\"); [Update a payment schedule](https://www.zuora.com/developer/api-references/api/operation/PUT_PaymentSchedule/ \"Update a payment schedule\"); [Update a payment schedule item](https://www.zuora.com/developer/api-references/api/operation/PUT_PaymentScheduleItem/ \"Update a payment schedule item\"); [Preview the result of payment schedule update](https://www.zuora.com/developer/api-references/api/operation/PUT_PaymentScheduleUpdatePreview/ \"Preview the result of payment schedule update\"); [Retrieve a payment schedule](https://www.zuora.com/developer/api-references/api/operation/GET_PaymentSchedule/ \"Retrieve a payment schedule\"); [Retrieve a payment schedule item](https://www.zuora.com/developer/api-references/api/operation/GET_PaymentScheduleItem/ \"Retrieve a payment schedule item\"); [List payment schedules by customer account](https://www.zuora.com/developer/api-references/api/operation/GET_PaymentSchedules/ \"List payment schedules by customer account\"); [Cancel a payment schedule](https://www.zuora.com/developer/api-references/api/operation/PUT_CancelPaymentSchedule/ \"Cancel a payment schedule\"); [Cancel a payment schedule item](https://www.zuora.com/developer/api-references/api/operation/PUT_CancelPaymentScheduleItem/ \"Cancel a payment schedule item\"); [Skip a payment schedule item](https://www.zuora.com/developer/api-references/api/operation/PUT_SkipPaymentScheduleItem/ \"Skip a payment schedule item\"); [Retry failed payment schedule items](https://www.zuora.com/developer/api-references/api/operation/POST_RetryPaymentScheduleItem/ \"Retry failed payment schedule items\"); [List payments](https://www.zuora.com/developer/api-references/api/operation/GET_RetrieveAllPayments/ \"List payments\") | Array of transactional level rules for processing payments. |    #### Version 207.0 and Later  The response structure of the [Preview Subscription](https://www.zuora.com/developer/api-references/api/operation/POST_PreviewSubscription) and [Update Subscription](https://www.zuora.com/developer/api-references/api/operation/PUT_Subscription \"Update Subscription\") methods are changed. The following invoice related response fields are moved to the invoice container:    * amount   * amountWithoutTax   * taxAmount   * invoiceItems   * targetDate   * chargeMetrics   # API Names for Zuora Objects  For information about the Zuora business object model, see [Zuora Business Object Model](https://www.zuora.com/developer/rest-api/general-concepts/object-model/).  You can use the [Describe](https://www.zuora.com/developer/api-references/api/operation/GET_Describe) operation to list the fields of each Zuora object that is available in your tenant. When you call the operation, you must specify the API name of the Zuora object.  The following table provides the API name of each Zuora object:  | Object                                        | API Name                                   | |-----------------------------------------------|--------------------------------------------| | Account                                       | `Account`                                  | | Accounting Code                               | `AccountingCode`                           | | Accounting Period                             | `AccountingPeriod`                         | | Amendment                                     | `Amendment`                                | | Application Group                             | `ApplicationGroup`                         | | Billing Run                                   | <p>`BillingRun` - API name used  in the [Describe](https://www.zuora.com/developer/api-references/api/operation/GET_Describe) operation, Export ZOQL queries, and Data Query.</p> <p>`BillRun` - API name used in the [Actions](https://www.zuora.com/developer/api-references/api/tag/Actions). See the CRUD oprations of [Bill Run](https://www.zuora.com/developer/api-references/api/tag/Bill-Run) for more information about the `BillRun` object. `BillingRun` and `BillRun` have different fields. |                      | Configuration Templates                       | `ConfigurationTemplates`                  | | Contact                                       | `Contact`                                  | | Contact Snapshot                              | `ContactSnapshot`                          | | Credit Balance Adjustment                     | `CreditBalanceAdjustment`                  | | Credit Memo                                   | `CreditMemo`                               | | Credit Memo Application                       | `CreditMemoApplication`                    | | Credit Memo Application Item                  | `CreditMemoApplicationItem`                | | Credit Memo Item                              | `CreditMemoItem`                           | | Credit Memo Part                              | `CreditMemoPart`                           | | Credit Memo Part Item                         | `CreditMemoPartItem`                       | | Credit Taxation Item                          | `CreditTaxationItem`                       | | Custom Exchange Rate                          | `FXCustomRate`                             | | Debit Memo                                    | `DebitMemo`                                | | Debit Memo Item                               | `DebitMemoItem`                            | | Debit Taxation Item                           | `DebitTaxationItem`                        | | Discount Applied Metrics                      | `DiscountAppliedMetrics`                   | | Entity                                        | `Tenant`                                   | | Fulfillment                                   | `Fulfillment`                              | | Feature                                       | `Feature`                                  | | Gateway Reconciliation Event                  | `PaymentGatewayReconciliationEventLog`     | | Gateway Reconciliation Job                    | `PaymentReconciliationJob`                 | | Gateway Reconciliation Log                    | `PaymentReconciliationLog`                 | | Invoice                                       | `Invoice`                                  | | Invoice Adjustment                            | `InvoiceAdjustment`                        | | Invoice Item                                  | `InvoiceItem`                              | | Invoice Item Adjustment                       | `InvoiceItemAdjustment`                    | | Invoice Payment                               | `InvoicePayment`                           | | Invoice Schedule                              | `InvoiceSchedule`                          | | Journal Entry                                 | `JournalEntry`                             | | Journal Entry Item                            | `JournalEntryItem`                         | | Journal Run                                   | `JournalRun`                               | | Notification History - Callout                | `CalloutHistory`                           | | Notification History - Email                  | `EmailHistory`                             | | Offer                                         | `Offer`                             | | Order                                         | `Order`                                    | | Order Action                                  | `OrderAction`                              | | Order ELP                                     | `OrderElp`                                 | | Order Line Items                              | `OrderLineItems`                           |     | Order Item                                    | `OrderItem`                                | | Order MRR                                     | `OrderMrr`                                 | | Order Quantity                                | `OrderQuantity`                            | | Order TCB                                     | `OrderTcb`                                 | | Order TCV                                     | `OrderTcv`                                 | | Payment                                       | `Payment`                                  | | Payment Application                           | `PaymentApplication`                       | | Payment Application Item                      | `PaymentApplicationItem`                   | | Payment Method                                | `PaymentMethod`                            | | Payment Method Snapshot                       | `PaymentMethodSnapshot`                    | | Payment Method Transaction Log                | `PaymentMethodTransactionLog`              | | Payment Method Update                        | `UpdaterDetail`                            | | Payment Part                                  | `PaymentPart`                              | | Payment Part Item                             | `PaymentPartItem`                          | | Payment Run                                   | `PaymentRun`                               | | Payment Transaction Log                       | `PaymentTransactionLog`                    | | Price Book Item                               | `PriceBookItem`                            | | Processed Usage                               | `ProcessedUsage`                           | | Product                                       | `Product`                                  | | Product Feature                               | `ProductFeature`                           | | Product Rate Plan                             | `ProductRatePlan`                          | | Product Rate Plan Charge                      | `ProductRatePlanCharge`                    | | Product Rate Plan Charge Tier                 | `ProductRatePlanChargeTier`                | | Rate Plan                                     | `RatePlan`                                 | | Rate Plan Charge                              | `RatePlanCharge`                           | | Rate Plan Charge Tier                         | `RatePlanChargeTier`                       | | Refund                                        | `Refund`                                   | | Refund Application                            | `RefundApplication`                        | | Refund Application Item                       | `RefundApplicationItem`                    | | Refund Invoice Payment                        | `RefundInvoicePayment`                     | | Refund Part                                   | `RefundPart`                               | | Refund Part Item                              | `RefundPartItem`                           | | Refund Transaction Log                        | `RefundTransactionLog`                     | | Revenue Charge Summary                        | `RevenueChargeSummary`                     | | Revenue Charge Summary Item                   | `RevenueChargeSummaryItem`                 | | Revenue Event                                 | `RevenueEvent`                             | | Revenue Event Credit Memo Item                | `RevenueEventCreditMemoItem`               | | Revenue Event Debit Memo Item                 | `RevenueEventDebitMemoItem`                | | Revenue Event Invoice Item                    | `RevenueEventInvoiceItem`                  | | Revenue Event Invoice Item Adjustment         | `RevenueEventInvoiceItemAdjustment`        | | Revenue Event Item                            | `RevenueEventItem`                         | | Revenue Event Item Credit Memo Item           | `RevenueEventItemCreditMemoItem`           | | Revenue Event Item Debit Memo Item            | `RevenueEventItemDebitMemoItem`            | | Revenue Event Item Invoice Item               | `RevenueEventItemInvoiceItem`              | | Revenue Event Item Invoice Item Adjustment    | `RevenueEventItemInvoiceItemAdjustment`    | | Revenue Event Type                            | `RevenueEventType`                         | | Revenue Schedule                              | `RevenueSchedule`                          | | Revenue Schedule Credit Memo Item             | `RevenueScheduleCreditMemoItem`            | | Revenue Schedule Debit Memo Item              | `RevenueScheduleDebitMemoItem`             | | Revenue Schedule Invoice Item                 | `RevenueScheduleInvoiceItem`               | | Revenue Schedule Invoice Item Adjustment      | `RevenueScheduleInvoiceItemAdjustment`     | | Revenue Schedule Item                         | `RevenueScheduleItem`                      | | Revenue Schedule Item Credit Memo Item        | `RevenueScheduleItemCreditMemoItem`        | | Revenue Schedule Item Debit Memo Item         | `RevenueScheduleItemDebitMemoItem`         | | Revenue Schedule Item Invoice Item            | `RevenueScheduleItemInvoiceItem`           | | Revenue Schedule Item Invoice Item Adjustment | `RevenueScheduleItemInvoiceItemAdjustment` | | Subscription                                  | `Subscription`                             | | Subscription Product Feature                  | `SubscriptionProductFeature`               | | Taxable Item Snapshot                         | `TaxableItemSnapshot`                      | | Taxation Item                                 | `TaxationItem`                             | | Updater Batch                                 | `UpdaterBatch`                             | | Usage                                         | `Usage`                                    |   # noqa: E501

    OpenAPI spec version: 2023-07-24
    Contact: docs@zuora.com
    Generated by: https://github.com/swagger-api/swagger-codegen.git
"""


import pprint
import re  # noqa: F401

import six

from swagger_client.configuration import Configuration


class CreateOrderCreateSubscription(object):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """

    """
    Attributes:
      swagger_types (dict): The key is attribute name
                            and the value is attribute type.
      attribute_map (dict): The key is attribute name
                            and the value is json key in definition.
    """
    swagger_types = {
        'bill_to_contact_id': 'str',
        'invoice_separately': 'bool',
        'invoice_template_id': 'str',
        'new_subscription_owner_account': 'CreateOrderCreateSubscriptionNewSubscriptionOwnerAccount',
        'notes': 'str',
        'payment_term': 'str',
        'sequence_set_id': 'str',
        'sold_to_contact_id': 'str',
        'subscribe_to_products': 'list[CreateSubscribeToProduct]',
        'subscribe_to_rate_plans': 'list[CreateOrderRatePlanOverride]',
        'subscription_number': 'str',
        'subscription_owner_account_number': 'str',
        'terms': 'CreateOrderCreateSubscriptionTerms'
    }

    attribute_map = {
        'bill_to_contact_id': 'billToContactId',
        'invoice_separately': 'invoiceSeparately',
        'invoice_template_id': 'invoiceTemplateId',
        'new_subscription_owner_account': 'newSubscriptionOwnerAccount',
        'notes': 'notes',
        'payment_term': 'paymentTerm',
        'sequence_set_id': 'sequenceSetId',
        'sold_to_contact_id': 'soldToContactId',
        'subscribe_to_products': 'subscribeToProducts',
        'subscribe_to_rate_plans': 'subscribeToRatePlans',
        'subscription_number': 'subscriptionNumber',
        'subscription_owner_account_number': 'subscriptionOwnerAccountNumber',
        'terms': 'terms'
    }

    def __init__(self, bill_to_contact_id=None, invoice_separately=None, invoice_template_id=None, new_subscription_owner_account=None, notes=None, payment_term=None, sequence_set_id=None, sold_to_contact_id=None, subscribe_to_products=None, subscribe_to_rate_plans=None, subscription_number=None, subscription_owner_account_number=None, terms=None, _configuration=None):  # noqa: E501
        """CreateOrderCreateSubscription - a model defined in Swagger"""  # noqa: E501
        if _configuration is None:
            _configuration = Configuration()
        self._configuration = _configuration

        self._bill_to_contact_id = None
        self._invoice_separately = None
        self._invoice_template_id = None
        self._new_subscription_owner_account = None
        self._notes = None
        self._payment_term = None
        self._sequence_set_id = None
        self._sold_to_contact_id = None
        self._subscribe_to_products = None
        self._subscribe_to_rate_plans = None
        self._subscription_number = None
        self._subscription_owner_account_number = None
        self._terms = None
        self.discriminator = None

        if bill_to_contact_id is not None:
            self.bill_to_contact_id = bill_to_contact_id
        if invoice_separately is not None:
            self.invoice_separately = invoice_separately
        if invoice_template_id is not None:
            self.invoice_template_id = invoice_template_id
        if new_subscription_owner_account is not None:
            self.new_subscription_owner_account = new_subscription_owner_account
        if notes is not None:
            self.notes = notes
        if payment_term is not None:
            self.payment_term = payment_term
        if sequence_set_id is not None:
            self.sequence_set_id = sequence_set_id
        if sold_to_contact_id is not None:
            self.sold_to_contact_id = sold_to_contact_id
        if subscribe_to_products is not None:
            self.subscribe_to_products = subscribe_to_products
        if subscribe_to_rate_plans is not None:
            self.subscribe_to_rate_plans = subscribe_to_rate_plans
        if subscription_number is not None:
            self.subscription_number = subscription_number
        if subscription_owner_account_number is not None:
            self.subscription_owner_account_number = subscription_owner_account_number
        if terms is not None:
            self.terms = terms

    @property
    def bill_to_contact_id(self):
        """Gets the bill_to_contact_id of this CreateOrderCreateSubscription.  # noqa: E501

        The ID of the bill-to contact associated with the subscription.  **Note**: If you have the [Flexible Billing Attributes](https://knowledgecenter.zuora.com/Billing/Subscriptions/Flexible_Billing_Attributes) feature disabled, this field is unavailable in the request body and the value of this field is `null` in the response body.   # noqa: E501

        :return: The bill_to_contact_id of this CreateOrderCreateSubscription.  # noqa: E501
        :rtype: str
        """
        return self._bill_to_contact_id

    @bill_to_contact_id.setter
    def bill_to_contact_id(self, bill_to_contact_id):
        """Sets the bill_to_contact_id of this CreateOrderCreateSubscription.

        The ID of the bill-to contact associated with the subscription.  **Note**: If you have the [Flexible Billing Attributes](https://knowledgecenter.zuora.com/Billing/Subscriptions/Flexible_Billing_Attributes) feature disabled, this field is unavailable in the request body and the value of this field is `null` in the response body.   # noqa: E501

        :param bill_to_contact_id: The bill_to_contact_id of this CreateOrderCreateSubscription.  # noqa: E501
        :type: str
        """

        self._bill_to_contact_id = bill_to_contact_id

    @property
    def invoice_separately(self):
        """Gets the invoice_separately of this CreateOrderCreateSubscription.  # noqa: E501

        Specifies whether the subscription appears on a separate invoice when Zuora generates invoices.   # noqa: E501

        :return: The invoice_separately of this CreateOrderCreateSubscription.  # noqa: E501
        :rtype: bool
        """
        return self._invoice_separately

    @invoice_separately.setter
    def invoice_separately(self, invoice_separately):
        """Sets the invoice_separately of this CreateOrderCreateSubscription.

        Specifies whether the subscription appears on a separate invoice when Zuora generates invoices.   # noqa: E501

        :param invoice_separately: The invoice_separately of this CreateOrderCreateSubscription.  # noqa: E501
        :type: bool
        """

        self._invoice_separately = invoice_separately

    @property
    def invoice_template_id(self):
        """Gets the invoice_template_id of this CreateOrderCreateSubscription.  # noqa: E501

        The ID of the invoice template associated with the subscription.  **Note**: If you have the [Flexible Billing Attributes](https://knowledgecenter.zuora.com/Billing/Subscriptions/Flexible_Billing_Attributes) feature disabled, this field is unavailable in the request body and the value of this field is `null` in the response body.   # noqa: E501

        :return: The invoice_template_id of this CreateOrderCreateSubscription.  # noqa: E501
        :rtype: str
        """
        return self._invoice_template_id

    @invoice_template_id.setter
    def invoice_template_id(self, invoice_template_id):
        """Sets the invoice_template_id of this CreateOrderCreateSubscription.

        The ID of the invoice template associated with the subscription.  **Note**: If you have the [Flexible Billing Attributes](https://knowledgecenter.zuora.com/Billing/Subscriptions/Flexible_Billing_Attributes) feature disabled, this field is unavailable in the request body and the value of this field is `null` in the response body.   # noqa: E501

        :param invoice_template_id: The invoice_template_id of this CreateOrderCreateSubscription.  # noqa: E501
        :type: str
        """

        self._invoice_template_id = invoice_template_id

    @property
    def new_subscription_owner_account(self):
        """Gets the new_subscription_owner_account of this CreateOrderCreateSubscription.  # noqa: E501


        :return: The new_subscription_owner_account of this CreateOrderCreateSubscription.  # noqa: E501
        :rtype: CreateOrderCreateSubscriptionNewSubscriptionOwnerAccount
        """
        return self._new_subscription_owner_account

    @new_subscription_owner_account.setter
    def new_subscription_owner_account(self, new_subscription_owner_account):
        """Sets the new_subscription_owner_account of this CreateOrderCreateSubscription.


        :param new_subscription_owner_account: The new_subscription_owner_account of this CreateOrderCreateSubscription.  # noqa: E501
        :type: CreateOrderCreateSubscriptionNewSubscriptionOwnerAccount
        """

        self._new_subscription_owner_account = new_subscription_owner_account

    @property
    def notes(self):
        """Gets the notes of this CreateOrderCreateSubscription.  # noqa: E501

        Notes about the subscription. These notes are only visible to Zuora users.   # noqa: E501

        :return: The notes of this CreateOrderCreateSubscription.  # noqa: E501
        :rtype: str
        """
        return self._notes

    @notes.setter
    def notes(self, notes):
        """Sets the notes of this CreateOrderCreateSubscription.

        Notes about the subscription. These notes are only visible to Zuora users.   # noqa: E501

        :param notes: The notes of this CreateOrderCreateSubscription.  # noqa: E501
        :type: str
        """
        if (self._configuration.client_side_validation and
                notes is not None and len(notes) > 500):
            raise ValueError("Invalid value for `notes`, length must be less than or equal to `500`")  # noqa: E501

        self._notes = notes

    @property
    def payment_term(self):
        """Gets the payment_term of this CreateOrderCreateSubscription.  # noqa: E501

        The name of the payment term associated with the subscription. For example, `Net 30`. The payment term determines the due dates of invoices.  **Note**: If you have the [Flexible Billing Attributes](https://knowledgecenter.zuora.com/Billing/Subscriptions/Flexible_Billing_Attributes) feature disabled, this field is unavailable in the request body and the value of this field is `null` in the response body.   # noqa: E501

        :return: The payment_term of this CreateOrderCreateSubscription.  # noqa: E501
        :rtype: str
        """
        return self._payment_term

    @payment_term.setter
    def payment_term(self, payment_term):
        """Sets the payment_term of this CreateOrderCreateSubscription.

        The name of the payment term associated with the subscription. For example, `Net 30`. The payment term determines the due dates of invoices.  **Note**: If you have the [Flexible Billing Attributes](https://knowledgecenter.zuora.com/Billing/Subscriptions/Flexible_Billing_Attributes) feature disabled, this field is unavailable in the request body and the value of this field is `null` in the response body.   # noqa: E501

        :param payment_term: The payment_term of this CreateOrderCreateSubscription.  # noqa: E501
        :type: str
        """

        self._payment_term = payment_term

    @property
    def sequence_set_id(self):
        """Gets the sequence_set_id of this CreateOrderCreateSubscription.  # noqa: E501

        The ID of the sequence set associated with the subscription.  **Note**: If you have the [Flexible Billing Attributes](https://knowledgecenter.zuora.com/Billing/Subscriptions/Flexible_Billing_Attributes) feature disabled, this field is unavailable in the request body and the value of this field is `null` in the response body.   # noqa: E501

        :return: The sequence_set_id of this CreateOrderCreateSubscription.  # noqa: E501
        :rtype: str
        """
        return self._sequence_set_id

    @sequence_set_id.setter
    def sequence_set_id(self, sequence_set_id):
        """Sets the sequence_set_id of this CreateOrderCreateSubscription.

        The ID of the sequence set associated with the subscription.  **Note**: If you have the [Flexible Billing Attributes](https://knowledgecenter.zuora.com/Billing/Subscriptions/Flexible_Billing_Attributes) feature disabled, this field is unavailable in the request body and the value of this field is `null` in the response body.   # noqa: E501

        :param sequence_set_id: The sequence_set_id of this CreateOrderCreateSubscription.  # noqa: E501
        :type: str
        """

        self._sequence_set_id = sequence_set_id

    @property
    def sold_to_contact_id(self):
        """Gets the sold_to_contact_id of this CreateOrderCreateSubscription.  # noqa: E501

        The ID of the sold-to contact associated with the subscription.  **Note**: If you have the [Flexible Billing Attributes](https://knowledgecenter.zuora.com/Billing/Subscriptions/Flexible_Billing_Attributes) feature disabled, this field is unavailable in the request body and the value of this field is `null` in the response body.   # noqa: E501

        :return: The sold_to_contact_id of this CreateOrderCreateSubscription.  # noqa: E501
        :rtype: str
        """
        return self._sold_to_contact_id

    @sold_to_contact_id.setter
    def sold_to_contact_id(self, sold_to_contact_id):
        """Sets the sold_to_contact_id of this CreateOrderCreateSubscription.

        The ID of the sold-to contact associated with the subscription.  **Note**: If you have the [Flexible Billing Attributes](https://knowledgecenter.zuora.com/Billing/Subscriptions/Flexible_Billing_Attributes) feature disabled, this field is unavailable in the request body and the value of this field is `null` in the response body.   # noqa: E501

        :param sold_to_contact_id: The sold_to_contact_id of this CreateOrderCreateSubscription.  # noqa: E501
        :type: str
        """

        self._sold_to_contact_id = sold_to_contact_id

    @property
    def subscribe_to_products(self):
        """Gets the subscribe_to_products of this CreateOrderCreateSubscription.  # noqa: E501

        List of offers or rate plans associated with the subscription.  - For a rate plans, the following fields are available:   - `chargeOverrides`   - `clearingExistingFeatures`   - `customFields`   - `externalCatalogPlanId`   - `externallyManagedPlanId`   - `productRatePlanId`   - `subscriptionProductFeatures`   - `uniqueToken` - For an offer, the following fields are available:   - `customFields`   - `productOfferId`   - `productOfferNumber`   - `ratePlanOverrides`   - `subscriptionOfferUniqueToken`   # noqa: E501

        :return: The subscribe_to_products of this CreateOrderCreateSubscription.  # noqa: E501
        :rtype: list[CreateSubscribeToProduct]
        """
        return self._subscribe_to_products

    @subscribe_to_products.setter
    def subscribe_to_products(self, subscribe_to_products):
        """Sets the subscribe_to_products of this CreateOrderCreateSubscription.

        List of offers or rate plans associated with the subscription.  - For a rate plans, the following fields are available:   - `chargeOverrides`   - `clearingExistingFeatures`   - `customFields`   - `externalCatalogPlanId`   - `externallyManagedPlanId`   - `productRatePlanId`   - `subscriptionProductFeatures`   - `uniqueToken` - For an offer, the following fields are available:   - `customFields`   - `productOfferId`   - `productOfferNumber`   - `ratePlanOverrides`   - `subscriptionOfferUniqueToken`   # noqa: E501

        :param subscribe_to_products: The subscribe_to_products of this CreateOrderCreateSubscription.  # noqa: E501
        :type: list[CreateSubscribeToProduct]
        """

        self._subscribe_to_products = subscribe_to_products

    @property
    def subscribe_to_rate_plans(self):
        """Gets the subscribe_to_rate_plans of this CreateOrderCreateSubscription.  # noqa: E501

        List of rate plans associated with the subscription.  **Note**: The `subscribeToRatePlans` field has been deprecated, this field is replaced by the `subscribeToProducts` field that supports both Rate Plans and Offers. In a new order request, you can use either `subscribeToRatePlans` or `subscribeToProducts`, not both.   # noqa: E501

        :return: The subscribe_to_rate_plans of this CreateOrderCreateSubscription.  # noqa: E501
        :rtype: list[CreateOrderRatePlanOverride]
        """
        return self._subscribe_to_rate_plans

    @subscribe_to_rate_plans.setter
    def subscribe_to_rate_plans(self, subscribe_to_rate_plans):
        """Sets the subscribe_to_rate_plans of this CreateOrderCreateSubscription.

        List of rate plans associated with the subscription.  **Note**: The `subscribeToRatePlans` field has been deprecated, this field is replaced by the `subscribeToProducts` field that supports both Rate Plans and Offers. In a new order request, you can use either `subscribeToRatePlans` or `subscribeToProducts`, not both.   # noqa: E501

        :param subscribe_to_rate_plans: The subscribe_to_rate_plans of this CreateOrderCreateSubscription.  # noqa: E501
        :type: list[CreateOrderRatePlanOverride]
        """

        self._subscribe_to_rate_plans = subscribe_to_rate_plans

    @property
    def subscription_number(self):
        """Gets the subscription_number of this CreateOrderCreateSubscription.  # noqa: E501

        Subscription number of the subscription. For example, A-S00000001.  If you do not set this field, Zuora will generate the subscription number.   # noqa: E501

        :return: The subscription_number of this CreateOrderCreateSubscription.  # noqa: E501
        :rtype: str
        """
        return self._subscription_number

    @subscription_number.setter
    def subscription_number(self, subscription_number):
        """Sets the subscription_number of this CreateOrderCreateSubscription.

        Subscription number of the subscription. For example, A-S00000001.  If you do not set this field, Zuora will generate the subscription number.   # noqa: E501

        :param subscription_number: The subscription_number of this CreateOrderCreateSubscription.  # noqa: E501
        :type: str
        """
        if (self._configuration.client_side_validation and
                subscription_number is not None and len(subscription_number) > 100):
            raise ValueError("Invalid value for `subscription_number`, length must be less than or equal to `100`")  # noqa: E501

        self._subscription_number = subscription_number

    @property
    def subscription_owner_account_number(self):
        """Gets the subscription_owner_account_number of this CreateOrderCreateSubscription.  # noqa: E501

        Account number of an existing account that will own the subscription. For example, A00000001.  If you do not set this field or the `newSubscriptionOwnerAccount` field, the account that owns the order will also own the subscription. Zuora will return an error if you set this field and the `newSubscriptionOwnerAccount` field.   # noqa: E501

        :return: The subscription_owner_account_number of this CreateOrderCreateSubscription.  # noqa: E501
        :rtype: str
        """
        return self._subscription_owner_account_number

    @subscription_owner_account_number.setter
    def subscription_owner_account_number(self, subscription_owner_account_number):
        """Sets the subscription_owner_account_number of this CreateOrderCreateSubscription.

        Account number of an existing account that will own the subscription. For example, A00000001.  If you do not set this field or the `newSubscriptionOwnerAccount` field, the account that owns the order will also own the subscription. Zuora will return an error if you set this field and the `newSubscriptionOwnerAccount` field.   # noqa: E501

        :param subscription_owner_account_number: The subscription_owner_account_number of this CreateOrderCreateSubscription.  # noqa: E501
        :type: str
        """
        if (self._configuration.client_side_validation and
                subscription_owner_account_number is not None and len(subscription_owner_account_number) > 70):
            raise ValueError("Invalid value for `subscription_owner_account_number`, length must be less than or equal to `70`")  # noqa: E501

        self._subscription_owner_account_number = subscription_owner_account_number

    @property
    def terms(self):
        """Gets the terms of this CreateOrderCreateSubscription.  # noqa: E501


        :return: The terms of this CreateOrderCreateSubscription.  # noqa: E501
        :rtype: CreateOrderCreateSubscriptionTerms
        """
        return self._terms

    @terms.setter
    def terms(self, terms):
        """Sets the terms of this CreateOrderCreateSubscription.


        :param terms: The terms of this CreateOrderCreateSubscription.  # noqa: E501
        :type: CreateOrderCreateSubscriptionTerms
        """

        self._terms = terms

    def to_dict(self):
        """Returns the model properties as a dict"""
        result = {}

        for attr, _ in six.iteritems(self.swagger_types):
            value = getattr(self, attr)
            if isinstance(value, list):
                result[attr] = list(map(
                    lambda x: x.to_dict() if hasattr(x, "to_dict") else x,
                    value
                ))
            elif hasattr(value, "to_dict"):
                result[attr] = value.to_dict()
            elif isinstance(value, dict):
                result[attr] = dict(map(
                    lambda item: (item[0], item[1].to_dict())
                    if hasattr(item[1], "to_dict") else item,
                    value.items()
                ))
            else:
                result[attr] = value
        if issubclass(CreateOrderCreateSubscription, dict):
            for key, value in self.items():
                result[key] = value

        return result

    def to_str(self):
        """Returns the string representation of the model"""
        return pprint.pformat(self.to_dict())

    def __repr__(self):
        """For `print` and `pprint`"""
        return self.to_str()

    def __eq__(self, other):
        """Returns true if both objects are equal"""
        if not isinstance(other, CreateOrderCreateSubscription):
            return False

        return self.to_dict() == other.to_dict()

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        if not isinstance(other, CreateOrderCreateSubscription):
            return True

        return self.to_dict() != other.to_dict()
