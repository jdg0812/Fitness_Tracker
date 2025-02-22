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


class OrderAction(object):
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
        'add_product': 'OrderActionAddProduct',
        'cancel_subscription': 'CancelSubscription',
        'change_plan': 'ChangePlan',
        'change_reason': 'str',
        'create_subscription': 'CreateSubscription',
        'custom_fields': 'OrderActionObjectCustomFields',
        'id': 'str',
        'order_items': 'list[OrderItem]',
        'order_metrics': 'list[OrderMetric]',
        'owner_transfer': 'OwnerTransfer',
        'remove_product': 'RemoveProduct',
        'renew_subscription': 'RenewSubscription',
        'resume': 'GetOrderResume',
        'sequence': 'int',
        'suspend': 'GetOrderSuspend',
        'terms_and_conditions': 'TermsAndConditions',
        'trigger_dates': 'list[TriggerDate]',
        'type': 'str',
        'update_product': 'OrderActionUpdateProduct'
    }

    attribute_map = {
        'add_product': 'addProduct',
        'cancel_subscription': 'cancelSubscription',
        'change_plan': 'changePlan',
        'change_reason': 'changeReason',
        'create_subscription': 'createSubscription',
        'custom_fields': 'customFields',
        'id': 'id',
        'order_items': 'orderItems',
        'order_metrics': 'orderMetrics',
        'owner_transfer': 'ownerTransfer',
        'remove_product': 'removeProduct',
        'renew_subscription': 'renewSubscription',
        'resume': 'resume',
        'sequence': 'sequence',
        'suspend': 'suspend',
        'terms_and_conditions': 'termsAndConditions',
        'trigger_dates': 'triggerDates',
        'type': 'type',
        'update_product': 'updateProduct'
    }

    def __init__(self, add_product=None, cancel_subscription=None, change_plan=None, change_reason=None, create_subscription=None, custom_fields=None, id=None, order_items=None, order_metrics=None, owner_transfer=None, remove_product=None, renew_subscription=None, resume=None, sequence=None, suspend=None, terms_and_conditions=None, trigger_dates=None, type=None, update_product=None, _configuration=None):  # noqa: E501
        """OrderAction - a model defined in Swagger"""  # noqa: E501
        if _configuration is None:
            _configuration = Configuration()
        self._configuration = _configuration

        self._add_product = None
        self._cancel_subscription = None
        self._change_plan = None
        self._change_reason = None
        self._create_subscription = None
        self._custom_fields = None
        self._id = None
        self._order_items = None
        self._order_metrics = None
        self._owner_transfer = None
        self._remove_product = None
        self._renew_subscription = None
        self._resume = None
        self._sequence = None
        self._suspend = None
        self._terms_and_conditions = None
        self._trigger_dates = None
        self._type = None
        self._update_product = None
        self.discriminator = None

        if add_product is not None:
            self.add_product = add_product
        if cancel_subscription is not None:
            self.cancel_subscription = cancel_subscription
        if change_plan is not None:
            self.change_plan = change_plan
        if change_reason is not None:
            self.change_reason = change_reason
        if create_subscription is not None:
            self.create_subscription = create_subscription
        if custom_fields is not None:
            self.custom_fields = custom_fields
        if id is not None:
            self.id = id
        if order_items is not None:
            self.order_items = order_items
        if order_metrics is not None:
            self.order_metrics = order_metrics
        if owner_transfer is not None:
            self.owner_transfer = owner_transfer
        if remove_product is not None:
            self.remove_product = remove_product
        if renew_subscription is not None:
            self.renew_subscription = renew_subscription
        if resume is not None:
            self.resume = resume
        if sequence is not None:
            self.sequence = sequence
        if suspend is not None:
            self.suspend = suspend
        if terms_and_conditions is not None:
            self.terms_and_conditions = terms_and_conditions
        if trigger_dates is not None:
            self.trigger_dates = trigger_dates
        if type is not None:
            self.type = type
        if update_product is not None:
            self.update_product = update_product

    @property
    def add_product(self):
        """Gets the add_product of this OrderAction.  # noqa: E501


        :return: The add_product of this OrderAction.  # noqa: E501
        :rtype: OrderActionAddProduct
        """
        return self._add_product

    @add_product.setter
    def add_product(self, add_product):
        """Sets the add_product of this OrderAction.


        :param add_product: The add_product of this OrderAction.  # noqa: E501
        :type: OrderActionAddProduct
        """

        self._add_product = add_product

    @property
    def cancel_subscription(self):
        """Gets the cancel_subscription of this OrderAction.  # noqa: E501


        :return: The cancel_subscription of this OrderAction.  # noqa: E501
        :rtype: CancelSubscription
        """
        return self._cancel_subscription

    @cancel_subscription.setter
    def cancel_subscription(self, cancel_subscription):
        """Sets the cancel_subscription of this OrderAction.


        :param cancel_subscription: The cancel_subscription of this OrderAction.  # noqa: E501
        :type: CancelSubscription
        """

        self._cancel_subscription = cancel_subscription

    @property
    def change_plan(self):
        """Gets the change_plan of this OrderAction.  # noqa: E501


        :return: The change_plan of this OrderAction.  # noqa: E501
        :rtype: ChangePlan
        """
        return self._change_plan

    @change_plan.setter
    def change_plan(self, change_plan):
        """Sets the change_plan of this OrderAction.


        :param change_plan: The change_plan of this OrderAction.  # noqa: E501
        :type: ChangePlan
        """

        self._change_plan = change_plan

    @property
    def change_reason(self):
        """Gets the change_reason of this OrderAction.  # noqa: E501

        The change reason set for an order action when an order is created.   # noqa: E501

        :return: The change_reason of this OrderAction.  # noqa: E501
        :rtype: str
        """
        return self._change_reason

    @change_reason.setter
    def change_reason(self, change_reason):
        """Sets the change_reason of this OrderAction.

        The change reason set for an order action when an order is created.   # noqa: E501

        :param change_reason: The change_reason of this OrderAction.  # noqa: E501
        :type: str
        """

        self._change_reason = change_reason

    @property
    def create_subscription(self):
        """Gets the create_subscription of this OrderAction.  # noqa: E501


        :return: The create_subscription of this OrderAction.  # noqa: E501
        :rtype: CreateSubscription
        """
        return self._create_subscription

    @create_subscription.setter
    def create_subscription(self, create_subscription):
        """Sets the create_subscription of this OrderAction.


        :param create_subscription: The create_subscription of this OrderAction.  # noqa: E501
        :type: CreateSubscription
        """

        self._create_subscription = create_subscription

    @property
    def custom_fields(self):
        """Gets the custom_fields of this OrderAction.  # noqa: E501


        :return: The custom_fields of this OrderAction.  # noqa: E501
        :rtype: OrderActionObjectCustomFields
        """
        return self._custom_fields

    @custom_fields.setter
    def custom_fields(self, custom_fields):
        """Sets the custom_fields of this OrderAction.


        :param custom_fields: The custom_fields of this OrderAction.  # noqa: E501
        :type: OrderActionObjectCustomFields
        """

        self._custom_fields = custom_fields

    @property
    def id(self):
        """Gets the id of this OrderAction.  # noqa: E501

        The Id of the order action processed in the order.  # noqa: E501

        :return: The id of this OrderAction.  # noqa: E501
        :rtype: str
        """
        return self._id

    @id.setter
    def id(self, id):
        """Sets the id of this OrderAction.

        The Id of the order action processed in the order.  # noqa: E501

        :param id: The id of this OrderAction.  # noqa: E501
        :type: str
        """

        self._id = id

    @property
    def order_items(self):
        """Gets the order_items of this OrderAction.  # noqa: E501

        The `orderItems` nested field is only available to existing Orders customers who already have access to the field.  **Note:** The following Order Metrics have been deprecated. Any new customers who onboard on [Orders](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/AA_Overview_of_Orders) or [Orders Harmonization](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/Orders_Harmonization/Orders_Harmonization) will not get these metrics. * The Order ELP and Order Item objects  * The \"Generated Reason\" and \"Order Item ID\" fields in the Order MRR, Order TCB, Order TCV, and Order Quantity objects  Existing Orders customers who have these metrics will continue to be supported.   # noqa: E501

        :return: The order_items of this OrderAction.  # noqa: E501
        :rtype: list[OrderItem]
        """
        return self._order_items

    @order_items.setter
    def order_items(self, order_items):
        """Sets the order_items of this OrderAction.

        The `orderItems` nested field is only available to existing Orders customers who already have access to the field.  **Note:** The following Order Metrics have been deprecated. Any new customers who onboard on [Orders](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/AA_Overview_of_Orders) or [Orders Harmonization](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/Orders_Harmonization/Orders_Harmonization) will not get these metrics. * The Order ELP and Order Item objects  * The \"Generated Reason\" and \"Order Item ID\" fields in the Order MRR, Order TCB, Order TCV, and Order Quantity objects  Existing Orders customers who have these metrics will continue to be supported.   # noqa: E501

        :param order_items: The order_items of this OrderAction.  # noqa: E501
        :type: list[OrderItem]
        """

        self._order_items = order_items

    @property
    def order_metrics(self):
        """Gets the order_metrics of this OrderAction.  # noqa: E501

        The container for order metrics.  **Note:** The following Order Metrics have been deprecated. Any new customers who onboard on [Orders](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/AA_Overview_of_Orders) or [Orders Harmonization](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/Orders_Harmonization/Orders_Harmonization) will not get these metrics. * The Order ELP and Order Item objects  * The \"Generated Reason\" and \"Order Item ID\" fields in the Order MRR, Order TCB, Order TCV, and Order Quantity objects  Existing Orders customers who have these metrics will continue to be supported.  **Note:** As of Zuora Billing Release 306, Zuora has upgraded the methodologies for calculating metrics in [Orders](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders). The new methodologies are reflected in the following Order Delta Metrics objects.  * [Order Delta Mrr](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/Order_Delta_Metrics/Order_Delta_Mrr) * [Order Delta Tcv](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/Order_Delta_Metrics/Order_Delta_Tcv) * [Order Delta Tcb](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/Order_Delta_Metrics/Order_Delta_Tcb)  It is recommended that all customers use the new [Order Delta Metrics](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/Order_Delta_Metrics/AA_Overview_of_Order_Delta_Metrics). If you are an existing [Order Metrics](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/AA_Overview_of_Orders/Key_Metrics_for_Orders) customer and want to migrate to Order Delta Metrics, submit a request at [Zuora Global Support](https://support.zuora.com/).  Whereas new customers, and existing customers not currently on [Order Metrics](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/AA_Overview_of_Orders/Key_Metrics_for_Orders), will no longer have access to Order Metrics, existing customers currently using Order Metrics will continue to be supported.   # noqa: E501

        :return: The order_metrics of this OrderAction.  # noqa: E501
        :rtype: list[OrderMetric]
        """
        return self._order_metrics

    @order_metrics.setter
    def order_metrics(self, order_metrics):
        """Sets the order_metrics of this OrderAction.

        The container for order metrics.  **Note:** The following Order Metrics have been deprecated. Any new customers who onboard on [Orders](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/AA_Overview_of_Orders) or [Orders Harmonization](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/Orders_Harmonization/Orders_Harmonization) will not get these metrics. * The Order ELP and Order Item objects  * The \"Generated Reason\" and \"Order Item ID\" fields in the Order MRR, Order TCB, Order TCV, and Order Quantity objects  Existing Orders customers who have these metrics will continue to be supported.  **Note:** As of Zuora Billing Release 306, Zuora has upgraded the methodologies for calculating metrics in [Orders](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders). The new methodologies are reflected in the following Order Delta Metrics objects.  * [Order Delta Mrr](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/Order_Delta_Metrics/Order_Delta_Mrr) * [Order Delta Tcv](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/Order_Delta_Metrics/Order_Delta_Tcv) * [Order Delta Tcb](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/Order_Delta_Metrics/Order_Delta_Tcb)  It is recommended that all customers use the new [Order Delta Metrics](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/Order_Delta_Metrics/AA_Overview_of_Order_Delta_Metrics). If you are an existing [Order Metrics](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/AA_Overview_of_Orders/Key_Metrics_for_Orders) customer and want to migrate to Order Delta Metrics, submit a request at [Zuora Global Support](https://support.zuora.com/).  Whereas new customers, and existing customers not currently on [Order Metrics](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/AA_Overview_of_Orders/Key_Metrics_for_Orders), will no longer have access to Order Metrics, existing customers currently using Order Metrics will continue to be supported.   # noqa: E501

        :param order_metrics: The order_metrics of this OrderAction.  # noqa: E501
        :type: list[OrderMetric]
        """

        self._order_metrics = order_metrics

    @property
    def owner_transfer(self):
        """Gets the owner_transfer of this OrderAction.  # noqa: E501


        :return: The owner_transfer of this OrderAction.  # noqa: E501
        :rtype: OwnerTransfer
        """
        return self._owner_transfer

    @owner_transfer.setter
    def owner_transfer(self, owner_transfer):
        """Sets the owner_transfer of this OrderAction.


        :param owner_transfer: The owner_transfer of this OrderAction.  # noqa: E501
        :type: OwnerTransfer
        """

        self._owner_transfer = owner_transfer

    @property
    def remove_product(self):
        """Gets the remove_product of this OrderAction.  # noqa: E501


        :return: The remove_product of this OrderAction.  # noqa: E501
        :rtype: RemoveProduct
        """
        return self._remove_product

    @remove_product.setter
    def remove_product(self, remove_product):
        """Sets the remove_product of this OrderAction.


        :param remove_product: The remove_product of this OrderAction.  # noqa: E501
        :type: RemoveProduct
        """

        self._remove_product = remove_product

    @property
    def renew_subscription(self):
        """Gets the renew_subscription of this OrderAction.  # noqa: E501


        :return: The renew_subscription of this OrderAction.  # noqa: E501
        :rtype: RenewSubscription
        """
        return self._renew_subscription

    @renew_subscription.setter
    def renew_subscription(self, renew_subscription):
        """Sets the renew_subscription of this OrderAction.


        :param renew_subscription: The renew_subscription of this OrderAction.  # noqa: E501
        :type: RenewSubscription
        """

        self._renew_subscription = renew_subscription

    @property
    def resume(self):
        """Gets the resume of this OrderAction.  # noqa: E501


        :return: The resume of this OrderAction.  # noqa: E501
        :rtype: GetOrderResume
        """
        return self._resume

    @resume.setter
    def resume(self, resume):
        """Sets the resume of this OrderAction.


        :param resume: The resume of this OrderAction.  # noqa: E501
        :type: GetOrderResume
        """

        self._resume = resume

    @property
    def sequence(self):
        """Gets the sequence of this OrderAction.  # noqa: E501

        The sequence of the order actions processed in the order.  # noqa: E501

        :return: The sequence of this OrderAction.  # noqa: E501
        :rtype: int
        """
        return self._sequence

    @sequence.setter
    def sequence(self, sequence):
        """Sets the sequence of this OrderAction.

        The sequence of the order actions processed in the order.  # noqa: E501

        :param sequence: The sequence of this OrderAction.  # noqa: E501
        :type: int
        """

        self._sequence = sequence

    @property
    def suspend(self):
        """Gets the suspend of this OrderAction.  # noqa: E501


        :return: The suspend of this OrderAction.  # noqa: E501
        :rtype: GetOrderSuspend
        """
        return self._suspend

    @suspend.setter
    def suspend(self, suspend):
        """Sets the suspend of this OrderAction.


        :param suspend: The suspend of this OrderAction.  # noqa: E501
        :type: GetOrderSuspend
        """

        self._suspend = suspend

    @property
    def terms_and_conditions(self):
        """Gets the terms_and_conditions of this OrderAction.  # noqa: E501


        :return: The terms_and_conditions of this OrderAction.  # noqa: E501
        :rtype: TermsAndConditions
        """
        return self._terms_and_conditions

    @terms_and_conditions.setter
    def terms_and_conditions(self, terms_and_conditions):
        """Sets the terms_and_conditions of this OrderAction.


        :param terms_and_conditions: The terms_and_conditions of this OrderAction.  # noqa: E501
        :type: TermsAndConditions
        """

        self._terms_and_conditions = terms_and_conditions

    @property
    def trigger_dates(self):
        """Gets the trigger_dates of this OrderAction.  # noqa: E501

        Container for the contract effective, service activation, and customer acceptance dates of the order action.   If [Zuora is configured to require service activation](https://knowledgecenter.zuora.com/CB_Billing/Billing_Settings/Define_Default_Subscription_Settings#Require_Service_Activation_of_Orders.3F) and the `ServiceActivation` field is not set for a `CreateSubscription` order action, a `Pending` order and a `Pending Activation` subscription are created.  If [Zuora is configured to require customer acceptance](https://knowledgecenter.zuora.com/CB_Billing/Billing_Settings/Define_Default_Subscription_Settings#Require_Customer_Acceptance_of_Orders.3F) and the `CustomerAcceptance` field is not set for a `CreateSubscription` order action, a `Pending` order and a `Pending Acceptance` subscription are created. At the same time, if the service activation date field is also required and not set, a `Pending` order and a `Pending Activation` subscription are created instead.  If [Zuora is configured to require service activation](https://knowledgecenter.zuora.com/CB_Billing/Billing_Settings/Define_Default_Subscription_Settings#Require_Service_Activation_of_Orders.3F) and the `ServiceActivation` field is not set for either of the following order actions, a `Pending` order is created. The subscription status is not impacted. **Note:** This feature is in **Limited Availability**. If you want to have access to the feature, submit a request at [Zuora Global Support](http://support.zuora.com/).  * AddProduct  * UpdateProduct  * RemoveProduct  * RenewSubscription  * TermsAndConditions  If [Zuora is configured to require customer acceptance](https://knowledgecenter.zuora.com/CB_Billing/Billing_Settings/Define_Default_Subscription_Settings#Require_Customer_Acceptance_of_Orders.3F) and the `CustomerAcceptance` field is not set for either of the following order actions, a `Pending` order is created. The subscription status is not impacted. **Note:** This feature is in **Limited Availability**. If you want to have access to the feature, submit a request at [Zuora Global Support](http://support.zuora.com/).  * AddProduct  * UpdateProduct  * RemoveProduct  * RenewSubscription  * TermsAndConditions   # noqa: E501

        :return: The trigger_dates of this OrderAction.  # noqa: E501
        :rtype: list[TriggerDate]
        """
        return self._trigger_dates

    @trigger_dates.setter
    def trigger_dates(self, trigger_dates):
        """Sets the trigger_dates of this OrderAction.

        Container for the contract effective, service activation, and customer acceptance dates of the order action.   If [Zuora is configured to require service activation](https://knowledgecenter.zuora.com/CB_Billing/Billing_Settings/Define_Default_Subscription_Settings#Require_Service_Activation_of_Orders.3F) and the `ServiceActivation` field is not set for a `CreateSubscription` order action, a `Pending` order and a `Pending Activation` subscription are created.  If [Zuora is configured to require customer acceptance](https://knowledgecenter.zuora.com/CB_Billing/Billing_Settings/Define_Default_Subscription_Settings#Require_Customer_Acceptance_of_Orders.3F) and the `CustomerAcceptance` field is not set for a `CreateSubscription` order action, a `Pending` order and a `Pending Acceptance` subscription are created. At the same time, if the service activation date field is also required and not set, a `Pending` order and a `Pending Activation` subscription are created instead.  If [Zuora is configured to require service activation](https://knowledgecenter.zuora.com/CB_Billing/Billing_Settings/Define_Default_Subscription_Settings#Require_Service_Activation_of_Orders.3F) and the `ServiceActivation` field is not set for either of the following order actions, a `Pending` order is created. The subscription status is not impacted. **Note:** This feature is in **Limited Availability**. If you want to have access to the feature, submit a request at [Zuora Global Support](http://support.zuora.com/).  * AddProduct  * UpdateProduct  * RemoveProduct  * RenewSubscription  * TermsAndConditions  If [Zuora is configured to require customer acceptance](https://knowledgecenter.zuora.com/CB_Billing/Billing_Settings/Define_Default_Subscription_Settings#Require_Customer_Acceptance_of_Orders.3F) and the `CustomerAcceptance` field is not set for either of the following order actions, a `Pending` order is created. The subscription status is not impacted. **Note:** This feature is in **Limited Availability**. If you want to have access to the feature, submit a request at [Zuora Global Support](http://support.zuora.com/).  * AddProduct  * UpdateProduct  * RemoveProduct  * RenewSubscription  * TermsAndConditions   # noqa: E501

        :param trigger_dates: The trigger_dates of this OrderAction.  # noqa: E501
        :type: list[TriggerDate]
        """

        self._trigger_dates = trigger_dates

    @property
    def type(self):
        """Gets the type of this OrderAction.  # noqa: E501

        Type of the order action.  **Note**: The change plan type of order action is currently not supported for Billing - Revenue Integration. When Billing - Revenue Integration is enabled, the change plan type of order action will no longer be applicable in Zuora Billing.   # noqa: E501

        :return: The type of this OrderAction.  # noqa: E501
        :rtype: str
        """
        return self._type

    @type.setter
    def type(self, type):
        """Sets the type of this OrderAction.

        Type of the order action.  **Note**: The change plan type of order action is currently not supported for Billing - Revenue Integration. When Billing - Revenue Integration is enabled, the change plan type of order action will no longer be applicable in Zuora Billing.   # noqa: E501

        :param type: The type of this OrderAction.  # noqa: E501
        :type: str
        """
        allowed_values = ["CreateSubscription", "TermsAndConditions", "AddProduct", "UpdateProduct", "RemoveProduct", "RenewSubscription", "CancelSubscription", "OwnerTransfer", "Suspend", "Resume", "ChangePlan"]  # noqa: E501
        if (self._configuration.client_side_validation and
                type not in allowed_values):
            raise ValueError(
                "Invalid value for `type` ({0}), must be one of {1}"  # noqa: E501
                .format(type, allowed_values)
            )

        self._type = type

    @property
    def update_product(self):
        """Gets the update_product of this OrderAction.  # noqa: E501


        :return: The update_product of this OrderAction.  # noqa: E501
        :rtype: OrderActionUpdateProduct
        """
        return self._update_product

    @update_product.setter
    def update_product(self, update_product):
        """Sets the update_product of this OrderAction.


        :param update_product: The update_product of this OrderAction.  # noqa: E501
        :type: OrderActionUpdateProduct
        """

        self._update_product = update_product

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
        if issubclass(OrderAction, dict):
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
        if not isinstance(other, OrderAction):
            return False

        return self.to_dict() == other.to_dict()

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        if not isinstance(other, OrderAction):
            return True

        return self.to_dict() != other.to_dict()
