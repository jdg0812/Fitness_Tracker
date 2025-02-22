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


class GETPublicEmailTemplateResponse(object):
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
        'active': 'bool',
        'bcc_email_address': 'str',
        'cc_email_address': 'str',
        'cc_email_type': 'str',
        'created_by': 'str',
        'created_on': 'datetime',
        'description': 'str',
        'email_body': 'str',
        'email_subject': 'str',
        'encoding_type': 'str',
        'event_category': 'float',
        'event_type_name': 'str',
        'event_type_namespace': 'str',
        'from_email_address': 'str',
        'from_email_type': 'str',
        'from_name': 'str',
        'id': 'str',
        'is_html': 'bool',
        'name': 'str',
        'reply_to_email_address': 'str',
        'reply_to_email_type': 'str',
        'to_email_address': 'str',
        'to_email_type': 'str',
        'updated_by': 'str',
        'updated_on': 'datetime'
    }

    attribute_map = {
        'active': 'active',
        'bcc_email_address': 'bccEmailAddress',
        'cc_email_address': 'ccEmailAddress',
        'cc_email_type': 'ccEmailType',
        'created_by': 'createdBy',
        'created_on': 'createdOn',
        'description': 'description',
        'email_body': 'emailBody',
        'email_subject': 'emailSubject',
        'encoding_type': 'encodingType',
        'event_category': 'eventCategory',
        'event_type_name': 'eventTypeName',
        'event_type_namespace': 'eventTypeNamespace',
        'from_email_address': 'fromEmailAddress',
        'from_email_type': 'fromEmailType',
        'from_name': 'fromName',
        'id': 'id',
        'is_html': 'isHtml',
        'name': 'name',
        'reply_to_email_address': 'replyToEmailAddress',
        'reply_to_email_type': 'replyToEmailType',
        'to_email_address': 'toEmailAddress',
        'to_email_type': 'toEmailType',
        'updated_by': 'updatedBy',
        'updated_on': 'updatedOn'
    }

    def __init__(self, active=None, bcc_email_address=None, cc_email_address=None, cc_email_type='SpecificEmails', created_by=None, created_on=None, description=None, email_body=None, email_subject=None, encoding_type=None, event_category=None, event_type_name=None, event_type_namespace=None, from_email_address=None, from_email_type=None, from_name=None, id=None, is_html=None, name=None, reply_to_email_address=None, reply_to_email_type=None, to_email_address=None, to_email_type=None, updated_by=None, updated_on=None, _configuration=None):  # noqa: E501
        """GETPublicEmailTemplateResponse - a model defined in Swagger"""  # noqa: E501
        if _configuration is None:
            _configuration = Configuration()
        self._configuration = _configuration

        self._active = None
        self._bcc_email_address = None
        self._cc_email_address = None
        self._cc_email_type = None
        self._created_by = None
        self._created_on = None
        self._description = None
        self._email_body = None
        self._email_subject = None
        self._encoding_type = None
        self._event_category = None
        self._event_type_name = None
        self._event_type_namespace = None
        self._from_email_address = None
        self._from_email_type = None
        self._from_name = None
        self._id = None
        self._is_html = None
        self._name = None
        self._reply_to_email_address = None
        self._reply_to_email_type = None
        self._to_email_address = None
        self._to_email_type = None
        self._updated_by = None
        self._updated_on = None
        self.discriminator = None

        if active is not None:
            self.active = active
        if bcc_email_address is not None:
            self.bcc_email_address = bcc_email_address
        if cc_email_address is not None:
            self.cc_email_address = cc_email_address
        if cc_email_type is not None:
            self.cc_email_type = cc_email_type
        if created_by is not None:
            self.created_by = created_by
        if created_on is not None:
            self.created_on = created_on
        if description is not None:
            self.description = description
        if email_body is not None:
            self.email_body = email_body
        if email_subject is not None:
            self.email_subject = email_subject
        if encoding_type is not None:
            self.encoding_type = encoding_type
        if event_category is not None:
            self.event_category = event_category
        if event_type_name is not None:
            self.event_type_name = event_type_name
        if event_type_namespace is not None:
            self.event_type_namespace = event_type_namespace
        if from_email_address is not None:
            self.from_email_address = from_email_address
        if from_email_type is not None:
            self.from_email_type = from_email_type
        if from_name is not None:
            self.from_name = from_name
        if id is not None:
            self.id = id
        if is_html is not None:
            self.is_html = is_html
        if name is not None:
            self.name = name
        if reply_to_email_address is not None:
            self.reply_to_email_address = reply_to_email_address
        if reply_to_email_type is not None:
            self.reply_to_email_type = reply_to_email_type
        if to_email_address is not None:
            self.to_email_address = to_email_address
        if to_email_type is not None:
            self.to_email_type = to_email_type
        if updated_by is not None:
            self.updated_by = updated_by
        if updated_on is not None:
            self.updated_on = updated_on

    @property
    def active(self):
        """Gets the active of this GETPublicEmailTemplateResponse.  # noqa: E501

        The status of the email template.  # noqa: E501

        :return: The active of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: bool
        """
        return self._active

    @active.setter
    def active(self, active):
        """Sets the active of this GETPublicEmailTemplateResponse.

        The status of the email template.  # noqa: E501

        :param active: The active of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: bool
        """

        self._active = active

    @property
    def bcc_email_address(self):
        """Gets the bcc_email_address of this GETPublicEmailTemplateResponse.  # noqa: E501

        Email BCC address.  # noqa: E501

        :return: The bcc_email_address of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: str
        """
        return self._bcc_email_address

    @bcc_email_address.setter
    def bcc_email_address(self, bcc_email_address):
        """Sets the bcc_email_address of this GETPublicEmailTemplateResponse.

        Email BCC address.  # noqa: E501

        :param bcc_email_address: The bcc_email_address of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: str
        """

        self._bcc_email_address = bcc_email_address

    @property
    def cc_email_address(self):
        """Gets the cc_email_address of this GETPublicEmailTemplateResponse.  # noqa: E501

        Email CC address.  # noqa: E501

        :return: The cc_email_address of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: str
        """
        return self._cc_email_address

    @cc_email_address.setter
    def cc_email_address(self, cc_email_address):
        """Sets the cc_email_address of this GETPublicEmailTemplateResponse.

        Email CC address.  # noqa: E501

        :param cc_email_address: The cc_email_address of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: str
        """

        self._cc_email_address = cc_email_address

    @property
    def cc_email_type(self):
        """Gets the cc_email_type of this GETPublicEmailTemplateResponse.  # noqa: E501

        Email cc type.  # noqa: E501

        :return: The cc_email_type of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: str
        """
        return self._cc_email_type

    @cc_email_type.setter
    def cc_email_type(self, cc_email_type):
        """Sets the cc_email_type of this GETPublicEmailTemplateResponse.

        Email cc type.  # noqa: E501

        :param cc_email_type: The cc_email_type of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: str
        """
        allowed_values = ["BillToContact", "SoldToContact", "SpecificEmails", "TenantAdmin", "BillToAndSoldToContacts", "RunOwner", "AllContacts", "InvoiceOwnerBillToContact", "InvoiceOwnerSoldToContact", "InvoiceOwnerBillToAndSoldToContacts", "InvoiceOwnerAllContacts"]  # noqa: E501
        if (self._configuration.client_side_validation and
                cc_email_type not in allowed_values):
            raise ValueError(
                "Invalid value for `cc_email_type` ({0}), must be one of {1}"  # noqa: E501
                .format(cc_email_type, allowed_values)
            )

        self._cc_email_type = cc_email_type

    @property
    def created_by(self):
        """Gets the created_by of this GETPublicEmailTemplateResponse.  # noqa: E501

        The ID of the user who created the email template.  # noqa: E501

        :return: The created_by of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: str
        """
        return self._created_by

    @created_by.setter
    def created_by(self, created_by):
        """Sets the created_by of this GETPublicEmailTemplateResponse.

        The ID of the user who created the email template.  # noqa: E501

        :param created_by: The created_by of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: str
        """

        self._created_by = created_by

    @property
    def created_on(self):
        """Gets the created_on of this GETPublicEmailTemplateResponse.  # noqa: E501

        The time when the email template was created. Specified in the UTC timezone in the ISO860 format (YYYY-MM-DDThh:mm:ss.sTZD). E.g. 1997-07-16T19:20:30.45+00:00  # noqa: E501

        :return: The created_on of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: datetime
        """
        return self._created_on

    @created_on.setter
    def created_on(self, created_on):
        """Sets the created_on of this GETPublicEmailTemplateResponse.

        The time when the email template was created. Specified in the UTC timezone in the ISO860 format (YYYY-MM-DDThh:mm:ss.sTZD). E.g. 1997-07-16T19:20:30.45+00:00  # noqa: E501

        :param created_on: The created_on of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: datetime
        """

        self._created_on = created_on

    @property
    def description(self):
        """Gets the description of this GETPublicEmailTemplateResponse.  # noqa: E501

        The description of the email template.  # noqa: E501

        :return: The description of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: str
        """
        return self._description

    @description.setter
    def description(self, description):
        """Sets the description of this GETPublicEmailTemplateResponse.

        The description of the email template.  # noqa: E501

        :param description: The description of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: str
        """
        if (self._configuration.client_side_validation and
                description is not None and len(description) > 255):
            raise ValueError("Invalid value for `description`, length must be less than or equal to `255`")  # noqa: E501

        self._description = description

    @property
    def email_body(self):
        """Gets the email_body of this GETPublicEmailTemplateResponse.  # noqa: E501

        The email body. You can add merge fields in the email object using angle brackets.  User can also embed html tags if `isHtml` is `true`.  # noqa: E501

        :return: The email_body of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: str
        """
        return self._email_body

    @email_body.setter
    def email_body(self, email_body):
        """Sets the email_body of this GETPublicEmailTemplateResponse.

        The email body. You can add merge fields in the email object using angle brackets.  User can also embed html tags if `isHtml` is `true`.  # noqa: E501

        :param email_body: The email_body of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: str
        """

        self._email_body = email_body

    @property
    def email_subject(self):
        """Gets the email_subject of this GETPublicEmailTemplateResponse.  # noqa: E501

        The email subject. You can add merge fields in the email subject using angle brackets.  # noqa: E501

        :return: The email_subject of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: str
        """
        return self._email_subject

    @email_subject.setter
    def email_subject(self, email_subject):
        """Sets the email_subject of this GETPublicEmailTemplateResponse.

        The email subject. You can add merge fields in the email subject using angle brackets.  # noqa: E501

        :param email_subject: The email_subject of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: str
        """

        self._email_subject = email_subject

    @property
    def encoding_type(self):
        """Gets the encoding_type of this GETPublicEmailTemplateResponse.  # noqa: E501

        The endcode type of the email body.  # noqa: E501

        :return: The encoding_type of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: str
        """
        return self._encoding_type

    @encoding_type.setter
    def encoding_type(self, encoding_type):
        """Sets the encoding_type of this GETPublicEmailTemplateResponse.

        The endcode type of the email body.  # noqa: E501

        :param encoding_type: The encoding_type of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: str
        """
        allowed_values = ["UTF8", "Shift_JIS", "ISO_2022_JP", "EUC_JP", "X_SJIS_0213"]  # noqa: E501
        if (self._configuration.client_side_validation and
                encoding_type not in allowed_values):
            raise ValueError(
                "Invalid value for `encoding_type` ({0}), must be one of {1}"  # noqa: E501
                .format(encoding_type, allowed_values)
            )

        self._encoding_type = encoding_type

    @property
    def event_category(self):
        """Gets the event_category of this GETPublicEmailTemplateResponse.  # noqa: E501

        The event category code for a standard event. See [Standard Event Categories](https://knowledgecenter.zuora.com/Central_Platform/Notifications/A_Standard_Events/Standard_Event_Category_Code_for_Notification_Histories_API) for all event category codes.  # noqa: E501

        :return: The event_category of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: float
        """
        return self._event_category

    @event_category.setter
    def event_category(self, event_category):
        """Sets the event_category of this GETPublicEmailTemplateResponse.

        The event category code for a standard event. See [Standard Event Categories](https://knowledgecenter.zuora.com/Central_Platform/Notifications/A_Standard_Events/Standard_Event_Category_Code_for_Notification_Histories_API) for all event category codes.  # noqa: E501

        :param event_category: The event_category of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: float
        """

        self._event_category = event_category

    @property
    def event_type_name(self):
        """Gets the event_type_name of this GETPublicEmailTemplateResponse.  # noqa: E501

        The name of the custom event or custom scheduled event.  # noqa: E501

        :return: The event_type_name of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: str
        """
        return self._event_type_name

    @event_type_name.setter
    def event_type_name(self, event_type_name):
        """Sets the event_type_name of this GETPublicEmailTemplateResponse.

        The name of the custom event or custom scheduled event.  # noqa: E501

        :param event_type_name: The event_type_name of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: str
        """
        if (self._configuration.client_side_validation and
                event_type_name is not None and len(event_type_name) < 1):
            raise ValueError("Invalid value for `event_type_name`, length must be greater than or equal to `1`")  # noqa: E501

        self._event_type_name = event_type_name

    @property
    def event_type_namespace(self):
        """Gets the event_type_namespace of this GETPublicEmailTemplateResponse.  # noqa: E501

        The namespace of the `eventTypeName` field for custom events and custom scheduled events.    # noqa: E501

        :return: The event_type_namespace of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: str
        """
        return self._event_type_namespace

    @event_type_namespace.setter
    def event_type_namespace(self, event_type_namespace):
        """Sets the event_type_namespace of this GETPublicEmailTemplateResponse.

        The namespace of the `eventTypeName` field for custom events and custom scheduled events.    # noqa: E501

        :param event_type_namespace: The event_type_namespace of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: str
        """

        self._event_type_namespace = event_type_namespace

    @property
    def from_email_address(self):
        """Gets the from_email_address of this GETPublicEmailTemplateResponse.  # noqa: E501

        If formEmailType is SpecificEmail, this field is required.  # noqa: E501

        :return: The from_email_address of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: str
        """
        return self._from_email_address

    @from_email_address.setter
    def from_email_address(self, from_email_address):
        """Sets the from_email_address of this GETPublicEmailTemplateResponse.

        If formEmailType is SpecificEmail, this field is required.  # noqa: E501

        :param from_email_address: The from_email_address of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: str
        """

        self._from_email_address = from_email_address

    @property
    def from_email_type(self):
        """Gets the from_email_type of this GETPublicEmailTemplateResponse.  # noqa: E501

        The from email type.  # noqa: E501

        :return: The from_email_type of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: str
        """
        return self._from_email_type

    @from_email_type.setter
    def from_email_type(self, from_email_type):
        """Sets the from_email_type of this GETPublicEmailTemplateResponse.

        The from email type.  # noqa: E501

        :param from_email_type: The from_email_type of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: str
        """
        allowed_values = ["TenantEmail", "RunOwner", "SpecificEmail"]  # noqa: E501
        if (self._configuration.client_side_validation and
                from_email_type not in allowed_values):
            raise ValueError(
                "Invalid value for `from_email_type` ({0}), must be one of {1}"  # noqa: E501
                .format(from_email_type, allowed_values)
            )

        self._from_email_type = from_email_type

    @property
    def from_name(self):
        """Gets the from_name of this GETPublicEmailTemplateResponse.  # noqa: E501

        The name of email sender.  # noqa: E501

        :return: The from_name of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: str
        """
        return self._from_name

    @from_name.setter
    def from_name(self, from_name):
        """Sets the from_name of this GETPublicEmailTemplateResponse.

        The name of email sender.  # noqa: E501

        :param from_name: The from_name of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: str
        """
        if (self._configuration.client_side_validation and
                from_name is not None and len(from_name) > 50):
            raise ValueError("Invalid value for `from_name`, length must be less than or equal to `50`")  # noqa: E501

        self._from_name = from_name

    @property
    def id(self):
        """Gets the id of this GETPublicEmailTemplateResponse.  # noqa: E501

        The email template ID.  # noqa: E501

        :return: The id of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: str
        """
        return self._id

    @id.setter
    def id(self, id):
        """Sets the id of this GETPublicEmailTemplateResponse.

        The email template ID.  # noqa: E501

        :param id: The id of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: str
        """

        self._id = id

    @property
    def is_html(self):
        """Gets the is_html of this GETPublicEmailTemplateResponse.  # noqa: E501

        Indicates whether the style of email body is HTML.  # noqa: E501

        :return: The is_html of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: bool
        """
        return self._is_html

    @is_html.setter
    def is_html(self, is_html):
        """Sets the is_html of this GETPublicEmailTemplateResponse.

        Indicates whether the style of email body is HTML.  # noqa: E501

        :param is_html: The is_html of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: bool
        """

        self._is_html = is_html

    @property
    def name(self):
        """Gets the name of this GETPublicEmailTemplateResponse.  # noqa: E501

        The name of the email template.  # noqa: E501

        :return: The name of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: str
        """
        return self._name

    @name.setter
    def name(self, name):
        """Sets the name of this GETPublicEmailTemplateResponse.

        The name of the email template.  # noqa: E501

        :param name: The name of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: str
        """
        if (self._configuration.client_side_validation and
                name is not None and len(name) > 255):
            raise ValueError("Invalid value for `name`, length must be less than or equal to `255`")  # noqa: E501

        self._name = name

    @property
    def reply_to_email_address(self):
        """Gets the reply_to_email_address of this GETPublicEmailTemplateResponse.  # noqa: E501

        If replyToEmailType is SpecificEmail, this field is required  # noqa: E501

        :return: The reply_to_email_address of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: str
        """
        return self._reply_to_email_address

    @reply_to_email_address.setter
    def reply_to_email_address(self, reply_to_email_address):
        """Sets the reply_to_email_address of this GETPublicEmailTemplateResponse.

        If replyToEmailType is SpecificEmail, this field is required  # noqa: E501

        :param reply_to_email_address: The reply_to_email_address of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: str
        """

        self._reply_to_email_address = reply_to_email_address

    @property
    def reply_to_email_type(self):
        """Gets the reply_to_email_type of this GETPublicEmailTemplateResponse.  # noqa: E501

        The reply email type.  # noqa: E501

        :return: The reply_to_email_type of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: str
        """
        return self._reply_to_email_type

    @reply_to_email_type.setter
    def reply_to_email_type(self, reply_to_email_type):
        """Sets the reply_to_email_type of this GETPublicEmailTemplateResponse.

        The reply email type.  # noqa: E501

        :param reply_to_email_type: The reply_to_email_type of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: str
        """
        allowed_values = ["TenantEmail", "RunOwner", "SpecificEmail"]  # noqa: E501
        if (self._configuration.client_side_validation and
                reply_to_email_type not in allowed_values):
            raise ValueError(
                "Invalid value for `reply_to_email_type` ({0}), must be one of {1}"  # noqa: E501
                .format(reply_to_email_type, allowed_values)
            )

        self._reply_to_email_type = reply_to_email_type

    @property
    def to_email_address(self):
        """Gets the to_email_address of this GETPublicEmailTemplateResponse.  # noqa: E501

        If `toEmailType` is `SpecificEmail`, this field is required.  # noqa: E501

        :return: The to_email_address of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: str
        """
        return self._to_email_address

    @to_email_address.setter
    def to_email_address(self, to_email_address):
        """Sets the to_email_address of this GETPublicEmailTemplateResponse.

        If `toEmailType` is `SpecificEmail`, this field is required.  # noqa: E501

        :param to_email_address: The to_email_address of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: str
        """

        self._to_email_address = to_email_address

    @property
    def to_email_type(self):
        """Gets the to_email_type of this GETPublicEmailTemplateResponse.  # noqa: E501

        Email receive type.  # noqa: E501

        :return: The to_email_type of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: str
        """
        return self._to_email_type

    @to_email_type.setter
    def to_email_type(self, to_email_type):
        """Sets the to_email_type of this GETPublicEmailTemplateResponse.

        Email receive type.  # noqa: E501

        :param to_email_type: The to_email_type of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: str
        """
        allowed_values = ["BillToContact", "SoldToContact", "SpecificEmails", "TenantAdmin", "BillToAndSoldToContacts", "RunOwner", "AllContacts", "InvoiceOwnerBillToContact", "InvoiceOwnerSoldToContact", "InvoiceOwnerBillToAndSoldToContacts", "InvoiceOwnerAllContacts"]  # noqa: E501
        if (self._configuration.client_side_validation and
                to_email_type not in allowed_values):
            raise ValueError(
                "Invalid value for `to_email_type` ({0}), must be one of {1}"  # noqa: E501
                .format(to_email_type, allowed_values)
            )

        self._to_email_type = to_email_type

    @property
    def updated_by(self):
        """Gets the updated_by of this GETPublicEmailTemplateResponse.  # noqa: E501

        The ID of the user who updated the email template.  # noqa: E501

        :return: The updated_by of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: str
        """
        return self._updated_by

    @updated_by.setter
    def updated_by(self, updated_by):
        """Sets the updated_by of this GETPublicEmailTemplateResponse.

        The ID of the user who updated the email template.  # noqa: E501

        :param updated_by: The updated_by of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: str
        """

        self._updated_by = updated_by

    @property
    def updated_on(self):
        """Gets the updated_on of this GETPublicEmailTemplateResponse.  # noqa: E501

        The time when the email template was updated. Specified in the UTC timezone in the ISO860 format (YYYY-MM-DDThh:mm:ss.sTZD). E.g. 1997-07-16T19:20:30.45+00:00  # noqa: E501

        :return: The updated_on of this GETPublicEmailTemplateResponse.  # noqa: E501
        :rtype: datetime
        """
        return self._updated_on

    @updated_on.setter
    def updated_on(self, updated_on):
        """Sets the updated_on of this GETPublicEmailTemplateResponse.

        The time when the email template was updated. Specified in the UTC timezone in the ISO860 format (YYYY-MM-DDThh:mm:ss.sTZD). E.g. 1997-07-16T19:20:30.45+00:00  # noqa: E501

        :param updated_on: The updated_on of this GETPublicEmailTemplateResponse.  # noqa: E501
        :type: datetime
        """

        self._updated_on = updated_on

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
        if issubclass(GETPublicEmailTemplateResponse, dict):
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
        if not isinstance(other, GETPublicEmailTemplateResponse):
            return False

        return self.to_dict() == other.to_dict()

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        if not isinstance(other, GETPublicEmailTemplateResponse):
            return True

        return self.to_dict() != other.to_dict()
