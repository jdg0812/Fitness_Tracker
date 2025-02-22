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


class OrderLineItemCommonRetrieveOrderLineItem(object):
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
        'uom': 'str',
        'accounting_code': 'str',
        'adjustment_liability_accounting_code': 'str',
        'adjustment_revenue_accounting_code': 'str',
        'amended_by_order_on': 'date',
        'amount_per_unit': 'float',
        'bill_target_date': 'date',
        'bill_to': 'str',
        'bill_to_snapshot_id': 'str',
        'billing_rule': 'str',
        'contract_asset_accounting_code': 'str',
        'contract_liability_accounting_code': 'str',
        'contract_recognized_revenue_accounting_code': 'str',
        'custom_fields': 'OrderLineItemCustomFieldsRetrieveOrderLineItem',
        'deferred_revenue_accounting_code': 'str',
        'description': 'str',
        'discount': 'float',
        'exclude_item_billing_from_revenue_accounting': 'bool',
        'exclude_item_booking_from_revenue_accounting': 'bool',
        'inline_discount_per_unit': 'float',
        'inline_discount_type': 'str',
        'invoice_owner_account_id': 'str',
        'invoice_owner_account_name': 'str',
        'invoice_owner_account_number': 'str',
        'is_allocation_eligible': 'bool',
        'is_unbilled': 'bool',
        'item_category': 'str',
        'item_name': 'str',
        'item_number': 'str',
        'item_state': 'str',
        'item_type': 'str',
        'list_price': 'float',
        'list_price_per_unit': 'float',
        'original_order_date': 'date',
        'original_order_id': 'str',
        'original_order_line_item_id': 'str',
        'original_order_line_item_number': 'str',
        'original_order_number': 'str',
        'owner_account_id': 'str',
        'owner_account_name': 'str',
        'owner_account_number': 'str',
        'product_code': 'str',
        'product_rate_plan_charge_id': 'str',
        'purchase_order_number': 'str',
        'quantity': 'float',
        'quantity_available_for_return': 'float',
        'quantity_fulfilled': 'float',
        'quantity_pending_fulfillment': 'float',
        'recognized_revenue_accounting_code': 'str',
        'related_subscription_number': 'str',
        'requires_fulfillment': 'bool',
        'revenue_recognition_rule': 'str',
        'sequence_set_id': 'str',
        'sold_to': 'str',
        'sold_to_snapshot_id': 'str',
        'tax_code': 'str',
        'tax_mode': 'str',
        'transaction_end_date': 'date',
        'transaction_start_date': 'date',
        'unbilled_receivables_accounting_code': 'str'
    }

    attribute_map = {
        'uom': 'UOM',
        'accounting_code': 'accountingCode',
        'adjustment_liability_accounting_code': 'adjustmentLiabilityAccountingCode',
        'adjustment_revenue_accounting_code': 'adjustmentRevenueAccountingCode',
        'amended_by_order_on': 'amendedByOrderOn',
        'amount_per_unit': 'amountPerUnit',
        'bill_target_date': 'billTargetDate',
        'bill_to': 'billTo',
        'bill_to_snapshot_id': 'billToSnapshotId',
        'billing_rule': 'billingRule',
        'contract_asset_accounting_code': 'contractAssetAccountingCode',
        'contract_liability_accounting_code': 'contractLiabilityAccountingCode',
        'contract_recognized_revenue_accounting_code': 'contractRecognizedRevenueAccountingCode',
        'custom_fields': 'customFields',
        'deferred_revenue_accounting_code': 'deferredRevenueAccountingCode',
        'description': 'description',
        'discount': 'discount',
        'exclude_item_billing_from_revenue_accounting': 'excludeItemBillingFromRevenueAccounting',
        'exclude_item_booking_from_revenue_accounting': 'excludeItemBookingFromRevenueAccounting',
        'inline_discount_per_unit': 'inlineDiscountPerUnit',
        'inline_discount_type': 'inlineDiscountType',
        'invoice_owner_account_id': 'invoiceOwnerAccountId',
        'invoice_owner_account_name': 'invoiceOwnerAccountName',
        'invoice_owner_account_number': 'invoiceOwnerAccountNumber',
        'is_allocation_eligible': 'isAllocationEligible',
        'is_unbilled': 'isUnbilled',
        'item_category': 'itemCategory',
        'item_name': 'itemName',
        'item_number': 'itemNumber',
        'item_state': 'itemState',
        'item_type': 'itemType',
        'list_price': 'listPrice',
        'list_price_per_unit': 'listPricePerUnit',
        'original_order_date': 'originalOrderDate',
        'original_order_id': 'originalOrderId',
        'original_order_line_item_id': 'originalOrderLineItemId',
        'original_order_line_item_number': 'originalOrderLineItemNumber',
        'original_order_number': 'originalOrderNumber',
        'owner_account_id': 'ownerAccountId',
        'owner_account_name': 'ownerAccountName',
        'owner_account_number': 'ownerAccountNumber',
        'product_code': 'productCode',
        'product_rate_plan_charge_id': 'productRatePlanChargeId',
        'purchase_order_number': 'purchaseOrderNumber',
        'quantity': 'quantity',
        'quantity_available_for_return': 'quantityAvailableForReturn',
        'quantity_fulfilled': 'quantityFulfilled',
        'quantity_pending_fulfillment': 'quantityPendingFulfillment',
        'recognized_revenue_accounting_code': 'recognizedRevenueAccountingCode',
        'related_subscription_number': 'relatedSubscriptionNumber',
        'requires_fulfillment': 'requiresFulfillment',
        'revenue_recognition_rule': 'revenueRecognitionRule',
        'sequence_set_id': 'sequenceSetId',
        'sold_to': 'soldTo',
        'sold_to_snapshot_id': 'soldToSnapshotId',
        'tax_code': 'taxCode',
        'tax_mode': 'taxMode',
        'transaction_end_date': 'transactionEndDate',
        'transaction_start_date': 'transactionStartDate',
        'unbilled_receivables_accounting_code': 'unbilledReceivablesAccountingCode'
    }

    def __init__(self, uom=None, accounting_code=None, adjustment_liability_accounting_code=None, adjustment_revenue_accounting_code=None, amended_by_order_on=None, amount_per_unit=None, bill_target_date=None, bill_to=None, bill_to_snapshot_id=None, billing_rule=None, contract_asset_accounting_code=None, contract_liability_accounting_code=None, contract_recognized_revenue_accounting_code=None, custom_fields=None, deferred_revenue_accounting_code=None, description=None, discount=None, exclude_item_billing_from_revenue_accounting=None, exclude_item_booking_from_revenue_accounting=None, inline_discount_per_unit=None, inline_discount_type=None, invoice_owner_account_id=None, invoice_owner_account_name=None, invoice_owner_account_number=None, is_allocation_eligible=None, is_unbilled=None, item_category='Sales', item_name=None, item_number=None, item_state=None, item_type=None, list_price=None, list_price_per_unit=None, original_order_date=None, original_order_id=None, original_order_line_item_id=None, original_order_line_item_number=None, original_order_number=None, owner_account_id=None, owner_account_name=None, owner_account_number=None, product_code=None, product_rate_plan_charge_id=None, purchase_order_number=None, quantity=None, quantity_available_for_return=None, quantity_fulfilled=None, quantity_pending_fulfillment=None, recognized_revenue_accounting_code=None, related_subscription_number=None, requires_fulfillment=None, revenue_recognition_rule=None, sequence_set_id=None, sold_to=None, sold_to_snapshot_id=None, tax_code=None, tax_mode=None, transaction_end_date=None, transaction_start_date=None, unbilled_receivables_accounting_code=None, _configuration=None):  # noqa: E501
        """OrderLineItemCommonRetrieveOrderLineItem - a model defined in Swagger"""  # noqa: E501
        if _configuration is None:
            _configuration = Configuration()
        self._configuration = _configuration

        self._uom = None
        self._accounting_code = None
        self._adjustment_liability_accounting_code = None
        self._adjustment_revenue_accounting_code = None
        self._amended_by_order_on = None
        self._amount_per_unit = None
        self._bill_target_date = None
        self._bill_to = None
        self._bill_to_snapshot_id = None
        self._billing_rule = None
        self._contract_asset_accounting_code = None
        self._contract_liability_accounting_code = None
        self._contract_recognized_revenue_accounting_code = None
        self._custom_fields = None
        self._deferred_revenue_accounting_code = None
        self._description = None
        self._discount = None
        self._exclude_item_billing_from_revenue_accounting = None
        self._exclude_item_booking_from_revenue_accounting = None
        self._inline_discount_per_unit = None
        self._inline_discount_type = None
        self._invoice_owner_account_id = None
        self._invoice_owner_account_name = None
        self._invoice_owner_account_number = None
        self._is_allocation_eligible = None
        self._is_unbilled = None
        self._item_category = None
        self._item_name = None
        self._item_number = None
        self._item_state = None
        self._item_type = None
        self._list_price = None
        self._list_price_per_unit = None
        self._original_order_date = None
        self._original_order_id = None
        self._original_order_line_item_id = None
        self._original_order_line_item_number = None
        self._original_order_number = None
        self._owner_account_id = None
        self._owner_account_name = None
        self._owner_account_number = None
        self._product_code = None
        self._product_rate_plan_charge_id = None
        self._purchase_order_number = None
        self._quantity = None
        self._quantity_available_for_return = None
        self._quantity_fulfilled = None
        self._quantity_pending_fulfillment = None
        self._recognized_revenue_accounting_code = None
        self._related_subscription_number = None
        self._requires_fulfillment = None
        self._revenue_recognition_rule = None
        self._sequence_set_id = None
        self._sold_to = None
        self._sold_to_snapshot_id = None
        self._tax_code = None
        self._tax_mode = None
        self._transaction_end_date = None
        self._transaction_start_date = None
        self._unbilled_receivables_accounting_code = None
        self.discriminator = None

        if uom is not None:
            self.uom = uom
        if accounting_code is not None:
            self.accounting_code = accounting_code
        if adjustment_liability_accounting_code is not None:
            self.adjustment_liability_accounting_code = adjustment_liability_accounting_code
        if adjustment_revenue_accounting_code is not None:
            self.adjustment_revenue_accounting_code = adjustment_revenue_accounting_code
        if amended_by_order_on is not None:
            self.amended_by_order_on = amended_by_order_on
        if amount_per_unit is not None:
            self.amount_per_unit = amount_per_unit
        if bill_target_date is not None:
            self.bill_target_date = bill_target_date
        if bill_to is not None:
            self.bill_to = bill_to
        if bill_to_snapshot_id is not None:
            self.bill_to_snapshot_id = bill_to_snapshot_id
        if billing_rule is not None:
            self.billing_rule = billing_rule
        if contract_asset_accounting_code is not None:
            self.contract_asset_accounting_code = contract_asset_accounting_code
        if contract_liability_accounting_code is not None:
            self.contract_liability_accounting_code = contract_liability_accounting_code
        if contract_recognized_revenue_accounting_code is not None:
            self.contract_recognized_revenue_accounting_code = contract_recognized_revenue_accounting_code
        if custom_fields is not None:
            self.custom_fields = custom_fields
        if deferred_revenue_accounting_code is not None:
            self.deferred_revenue_accounting_code = deferred_revenue_accounting_code
        if description is not None:
            self.description = description
        if discount is not None:
            self.discount = discount
        if exclude_item_billing_from_revenue_accounting is not None:
            self.exclude_item_billing_from_revenue_accounting = exclude_item_billing_from_revenue_accounting
        if exclude_item_booking_from_revenue_accounting is not None:
            self.exclude_item_booking_from_revenue_accounting = exclude_item_booking_from_revenue_accounting
        if inline_discount_per_unit is not None:
            self.inline_discount_per_unit = inline_discount_per_unit
        if inline_discount_type is not None:
            self.inline_discount_type = inline_discount_type
        if invoice_owner_account_id is not None:
            self.invoice_owner_account_id = invoice_owner_account_id
        if invoice_owner_account_name is not None:
            self.invoice_owner_account_name = invoice_owner_account_name
        if invoice_owner_account_number is not None:
            self.invoice_owner_account_number = invoice_owner_account_number
        if is_allocation_eligible is not None:
            self.is_allocation_eligible = is_allocation_eligible
        if is_unbilled is not None:
            self.is_unbilled = is_unbilled
        if item_category is not None:
            self.item_category = item_category
        if item_name is not None:
            self.item_name = item_name
        if item_number is not None:
            self.item_number = item_number
        if item_state is not None:
            self.item_state = item_state
        if item_type is not None:
            self.item_type = item_type
        if list_price is not None:
            self.list_price = list_price
        if list_price_per_unit is not None:
            self.list_price_per_unit = list_price_per_unit
        if original_order_date is not None:
            self.original_order_date = original_order_date
        if original_order_id is not None:
            self.original_order_id = original_order_id
        if original_order_line_item_id is not None:
            self.original_order_line_item_id = original_order_line_item_id
        if original_order_line_item_number is not None:
            self.original_order_line_item_number = original_order_line_item_number
        if original_order_number is not None:
            self.original_order_number = original_order_number
        if owner_account_id is not None:
            self.owner_account_id = owner_account_id
        if owner_account_name is not None:
            self.owner_account_name = owner_account_name
        if owner_account_number is not None:
            self.owner_account_number = owner_account_number
        if product_code is not None:
            self.product_code = product_code
        if product_rate_plan_charge_id is not None:
            self.product_rate_plan_charge_id = product_rate_plan_charge_id
        if purchase_order_number is not None:
            self.purchase_order_number = purchase_order_number
        if quantity is not None:
            self.quantity = quantity
        if quantity_available_for_return is not None:
            self.quantity_available_for_return = quantity_available_for_return
        if quantity_fulfilled is not None:
            self.quantity_fulfilled = quantity_fulfilled
        if quantity_pending_fulfillment is not None:
            self.quantity_pending_fulfillment = quantity_pending_fulfillment
        if recognized_revenue_accounting_code is not None:
            self.recognized_revenue_accounting_code = recognized_revenue_accounting_code
        if related_subscription_number is not None:
            self.related_subscription_number = related_subscription_number
        if requires_fulfillment is not None:
            self.requires_fulfillment = requires_fulfillment
        if revenue_recognition_rule is not None:
            self.revenue_recognition_rule = revenue_recognition_rule
        if sequence_set_id is not None:
            self.sequence_set_id = sequence_set_id
        if sold_to is not None:
            self.sold_to = sold_to
        if sold_to_snapshot_id is not None:
            self.sold_to_snapshot_id = sold_to_snapshot_id
        if tax_code is not None:
            self.tax_code = tax_code
        if tax_mode is not None:
            self.tax_mode = tax_mode
        if transaction_end_date is not None:
            self.transaction_end_date = transaction_end_date
        if transaction_start_date is not None:
            self.transaction_start_date = transaction_start_date
        if unbilled_receivables_accounting_code is not None:
            self.unbilled_receivables_accounting_code = unbilled_receivables_accounting_code

    @property
    def uom(self):
        """Gets the uom of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        Specifies the units to measure usage.   # noqa: E501

        :return: The uom of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._uom

    @uom.setter
    def uom(self, uom):
        """Sets the uom of this OrderLineItemCommonRetrieveOrderLineItem.

        Specifies the units to measure usage.   # noqa: E501

        :param uom: The uom of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._uom = uom

    @property
    def accounting_code(self):
        """Gets the accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The accountingCode for the Order Line Item.   # noqa: E501

        :return: The accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._accounting_code

    @accounting_code.setter
    def accounting_code(self, accounting_code):
        """Sets the accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.

        The accountingCode for the Order Line Item.   # noqa: E501

        :param accounting_code: The accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._accounting_code = accounting_code

    @property
    def adjustment_liability_accounting_code(self):
        """Gets the adjustment_liability_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The accounting code on the Order Line Item object for customers using [Zuora Billing - Revenue Integration](https://knowledgecenter.zuora.com/Zuora_Revenue/Zuora_Billing_-_Revenue_Integration).   # noqa: E501

        :return: The adjustment_liability_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._adjustment_liability_accounting_code

    @adjustment_liability_accounting_code.setter
    def adjustment_liability_accounting_code(self, adjustment_liability_accounting_code):
        """Sets the adjustment_liability_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.

        The accounting code on the Order Line Item object for customers using [Zuora Billing - Revenue Integration](https://knowledgecenter.zuora.com/Zuora_Revenue/Zuora_Billing_-_Revenue_Integration).   # noqa: E501

        :param adjustment_liability_accounting_code: The adjustment_liability_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._adjustment_liability_accounting_code = adjustment_liability_accounting_code

    @property
    def adjustment_revenue_accounting_code(self):
        """Gets the adjustment_revenue_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The accounting code on the Order Line Item object for customers using [Zuora Billing - Revenue Integration](https://knowledgecenter.zuora.com/Zuora_Revenue/Zuora_Billing_-_Revenue_Integration).   # noqa: E501

        :return: The adjustment_revenue_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._adjustment_revenue_accounting_code

    @adjustment_revenue_accounting_code.setter
    def adjustment_revenue_accounting_code(self, adjustment_revenue_accounting_code):
        """Sets the adjustment_revenue_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.

        The accounting code on the Order Line Item object for customers using [Zuora Billing - Revenue Integration](https://knowledgecenter.zuora.com/Zuora_Revenue/Zuora_Billing_-_Revenue_Integration).   # noqa: E501

        :param adjustment_revenue_accounting_code: The adjustment_revenue_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._adjustment_revenue_accounting_code = adjustment_revenue_accounting_code

    @property
    def amended_by_order_on(self):
        """Gets the amended_by_order_on of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The date when the rate plan charge is amended through an order or amendment. This field is to standardize the booking date information to increase audit ability and traceability of data between Zuora Billing and Zuora Revenue. It is mapped as the booking date for a sales order line in Zuora Revenue.   # noqa: E501

        :return: The amended_by_order_on of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: date
        """
        return self._amended_by_order_on

    @amended_by_order_on.setter
    def amended_by_order_on(self, amended_by_order_on):
        """Sets the amended_by_order_on of this OrderLineItemCommonRetrieveOrderLineItem.

        The date when the rate plan charge is amended through an order or amendment. This field is to standardize the booking date information to increase audit ability and traceability of data between Zuora Billing and Zuora Revenue. It is mapped as the booking date for a sales order line in Zuora Revenue.   # noqa: E501

        :param amended_by_order_on: The amended_by_order_on of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: date
        """

        self._amended_by_order_on = amended_by_order_on

    @property
    def amount_per_unit(self):
        """Gets the amount_per_unit of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The actual charged amount per unit for the Order Line Item.   # noqa: E501

        :return: The amount_per_unit of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: float
        """
        return self._amount_per_unit

    @amount_per_unit.setter
    def amount_per_unit(self, amount_per_unit):
        """Sets the amount_per_unit of this OrderLineItemCommonRetrieveOrderLineItem.

        The actual charged amount per unit for the Order Line Item.   # noqa: E501

        :param amount_per_unit: The amount_per_unit of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: float
        """

        self._amount_per_unit = amount_per_unit

    @property
    def bill_target_date(self):
        """Gets the bill_target_date of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The target date for the Order Line Item to be picked up by bill run for billing.   # noqa: E501

        :return: The bill_target_date of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: date
        """
        return self._bill_target_date

    @bill_target_date.setter
    def bill_target_date(self, bill_target_date):
        """Sets the bill_target_date of this OrderLineItemCommonRetrieveOrderLineItem.

        The target date for the Order Line Item to be picked up by bill run for billing.   # noqa: E501

        :param bill_target_date: The bill_target_date of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: date
        """

        self._bill_target_date = bill_target_date

    @property
    def bill_to(self):
        """Gets the bill_to of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The ID of a contact that belongs to the billing account of the order line item. Use this field to assign an existing account as the bill-to contact of an order line item.   # noqa: E501

        :return: The bill_to of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._bill_to

    @bill_to.setter
    def bill_to(self, bill_to):
        """Sets the bill_to of this OrderLineItemCommonRetrieveOrderLineItem.

        The ID of a contact that belongs to the billing account of the order line item. Use this field to assign an existing account as the bill-to contact of an order line item.   # noqa: E501

        :param bill_to: The bill_to of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._bill_to = bill_to

    @property
    def bill_to_snapshot_id(self):
        """Gets the bill_to_snapshot_id of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The snapshot of the ID for an account used as the bill-to contact of an order line item. This field is used to store the original information about the account, in case the information about the account is changed after the creation of the order line item. The `billToSnapshotId` field is exposed while retrieving the order line item details.   # noqa: E501

        :return: The bill_to_snapshot_id of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._bill_to_snapshot_id

    @bill_to_snapshot_id.setter
    def bill_to_snapshot_id(self, bill_to_snapshot_id):
        """Sets the bill_to_snapshot_id of this OrderLineItemCommonRetrieveOrderLineItem.

        The snapshot of the ID for an account used as the bill-to contact of an order line item. This field is used to store the original information about the account, in case the information about the account is changed after the creation of the order line item. The `billToSnapshotId` field is exposed while retrieving the order line item details.   # noqa: E501

        :param bill_to_snapshot_id: The bill_to_snapshot_id of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._bill_to_snapshot_id = bill_to_snapshot_id

    @property
    def billing_rule(self):
        """Gets the billing_rule of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The billing rule for the Order Line Item.   # noqa: E501

        :return: The billing_rule of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._billing_rule

    @billing_rule.setter
    def billing_rule(self, billing_rule):
        """Sets the billing_rule of this OrderLineItemCommonRetrieveOrderLineItem.

        The billing rule for the Order Line Item.   # noqa: E501

        :param billing_rule: The billing_rule of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """
        allowed_values = ["TriggerWithoutFulfillment", "TriggerAsFulfillmentOccurs"]  # noqa: E501
        if (self._configuration.client_side_validation and
                billing_rule not in allowed_values):
            raise ValueError(
                "Invalid value for `billing_rule` ({0}), must be one of {1}"  # noqa: E501
                .format(billing_rule, allowed_values)
            )

        self._billing_rule = billing_rule

    @property
    def contract_asset_accounting_code(self):
        """Gets the contract_asset_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The accounting code on the Order Line Item object for customers using [Zuora Billing - Revenue Integration](https://knowledgecenter.zuora.com/Zuora_Revenue/Zuora_Billing_-_Revenue_Integration).   # noqa: E501

        :return: The contract_asset_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._contract_asset_accounting_code

    @contract_asset_accounting_code.setter
    def contract_asset_accounting_code(self, contract_asset_accounting_code):
        """Sets the contract_asset_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.

        The accounting code on the Order Line Item object for customers using [Zuora Billing - Revenue Integration](https://knowledgecenter.zuora.com/Zuora_Revenue/Zuora_Billing_-_Revenue_Integration).   # noqa: E501

        :param contract_asset_accounting_code: The contract_asset_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._contract_asset_accounting_code = contract_asset_accounting_code

    @property
    def contract_liability_accounting_code(self):
        """Gets the contract_liability_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The accounting code on the Order Line Item object for customers using [Zuora Billing - Revenue Integration](https://knowledgecenter.zuora.com/Zuora_Revenue/Zuora_Billing_-_Revenue_Integration).   # noqa: E501

        :return: The contract_liability_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._contract_liability_accounting_code

    @contract_liability_accounting_code.setter
    def contract_liability_accounting_code(self, contract_liability_accounting_code):
        """Sets the contract_liability_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.

        The accounting code on the Order Line Item object for customers using [Zuora Billing - Revenue Integration](https://knowledgecenter.zuora.com/Zuora_Revenue/Zuora_Billing_-_Revenue_Integration).   # noqa: E501

        :param contract_liability_accounting_code: The contract_liability_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._contract_liability_accounting_code = contract_liability_accounting_code

    @property
    def contract_recognized_revenue_accounting_code(self):
        """Gets the contract_recognized_revenue_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The accounting code on the Order Line Item object for customers using [Zuora Billing - Revenue Integration](https://knowledgecenter.zuora.com/Zuora_Revenue/Zuora_Billing_-_Revenue_Integration).   # noqa: E501

        :return: The contract_recognized_revenue_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._contract_recognized_revenue_accounting_code

    @contract_recognized_revenue_accounting_code.setter
    def contract_recognized_revenue_accounting_code(self, contract_recognized_revenue_accounting_code):
        """Sets the contract_recognized_revenue_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.

        The accounting code on the Order Line Item object for customers using [Zuora Billing - Revenue Integration](https://knowledgecenter.zuora.com/Zuora_Revenue/Zuora_Billing_-_Revenue_Integration).   # noqa: E501

        :param contract_recognized_revenue_accounting_code: The contract_recognized_revenue_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._contract_recognized_revenue_accounting_code = contract_recognized_revenue_accounting_code

    @property
    def custom_fields(self):
        """Gets the custom_fields of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501


        :return: The custom_fields of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: OrderLineItemCustomFieldsRetrieveOrderLineItem
        """
        return self._custom_fields

    @custom_fields.setter
    def custom_fields(self, custom_fields):
        """Sets the custom_fields of this OrderLineItemCommonRetrieveOrderLineItem.


        :param custom_fields: The custom_fields of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: OrderLineItemCustomFieldsRetrieveOrderLineItem
        """

        self._custom_fields = custom_fields

    @property
    def deferred_revenue_accounting_code(self):
        """Gets the deferred_revenue_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The deferred revenue accounting code for the Order Line Item.   # noqa: E501

        :return: The deferred_revenue_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._deferred_revenue_accounting_code

    @deferred_revenue_accounting_code.setter
    def deferred_revenue_accounting_code(self, deferred_revenue_accounting_code):
        """Sets the deferred_revenue_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.

        The deferred revenue accounting code for the Order Line Item.   # noqa: E501

        :param deferred_revenue_accounting_code: The deferred_revenue_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._deferred_revenue_accounting_code = deferred_revenue_accounting_code

    @property
    def description(self):
        """Gets the description of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The description of the Order Line Item.   # noqa: E501

        :return: The description of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._description

    @description.setter
    def description(self, description):
        """Sets the description of this OrderLineItemCommonRetrieveOrderLineItem.

        The description of the Order Line Item.   # noqa: E501

        :param description: The description of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._description = description

    @property
    def discount(self):
        """Gets the discount of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        This field shows the total discount amount that is applied to an order line item after the `inlineDiscountType`, `inlineDiscountPerUnit` and `quantity` fields are set.  The inline discount is applied to the list price of an order line item (see the `listPrice` field).   # noqa: E501

        :return: The discount of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: float
        """
        return self._discount

    @discount.setter
    def discount(self, discount):
        """Sets the discount of this OrderLineItemCommonRetrieveOrderLineItem.

        This field shows the total discount amount that is applied to an order line item after the `inlineDiscountType`, `inlineDiscountPerUnit` and `quantity` fields are set.  The inline discount is applied to the list price of an order line item (see the `listPrice` field).   # noqa: E501

        :param discount: The discount of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: float
        """

        self._discount = discount

    @property
    def exclude_item_billing_from_revenue_accounting(self):
        """Gets the exclude_item_billing_from_revenue_accounting of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The flag to exclude Order Line Item related invoice items, invoice item adjustments, credit memo items, and debit memo items from revenue accounting.  **Note**: This field is only available if you have the [Zuora Billing - Revenue Integration](https://knowledgecenter.zuora.com/Zuora_Revenue/Zuora_Billing_-_Revenue_Integration) feature enabled.    # noqa: E501

        :return: The exclude_item_billing_from_revenue_accounting of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: bool
        """
        return self._exclude_item_billing_from_revenue_accounting

    @exclude_item_billing_from_revenue_accounting.setter
    def exclude_item_billing_from_revenue_accounting(self, exclude_item_billing_from_revenue_accounting):
        """Sets the exclude_item_billing_from_revenue_accounting of this OrderLineItemCommonRetrieveOrderLineItem.

        The flag to exclude Order Line Item related invoice items, invoice item adjustments, credit memo items, and debit memo items from revenue accounting.  **Note**: This field is only available if you have the [Zuora Billing - Revenue Integration](https://knowledgecenter.zuora.com/Zuora_Revenue/Zuora_Billing_-_Revenue_Integration) feature enabled.    # noqa: E501

        :param exclude_item_billing_from_revenue_accounting: The exclude_item_billing_from_revenue_accounting of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: bool
        """

        self._exclude_item_billing_from_revenue_accounting = exclude_item_billing_from_revenue_accounting

    @property
    def exclude_item_booking_from_revenue_accounting(self):
        """Gets the exclude_item_booking_from_revenue_accounting of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The flag to exclude Order Line Item from revenue accounting.  **Note**: This field is only available if you have the [Zuora Billing - Revenue Integration](https://knowledgecenter.zuora.com/Zuora_Revenue/Zuora_Billing_-_Revenue_Integration) feature enabled.    # noqa: E501

        :return: The exclude_item_booking_from_revenue_accounting of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: bool
        """
        return self._exclude_item_booking_from_revenue_accounting

    @exclude_item_booking_from_revenue_accounting.setter
    def exclude_item_booking_from_revenue_accounting(self, exclude_item_booking_from_revenue_accounting):
        """Sets the exclude_item_booking_from_revenue_accounting of this OrderLineItemCommonRetrieveOrderLineItem.

        The flag to exclude Order Line Item from revenue accounting.  **Note**: This field is only available if you have the [Zuora Billing - Revenue Integration](https://knowledgecenter.zuora.com/Zuora_Revenue/Zuora_Billing_-_Revenue_Integration) feature enabled.    # noqa: E501

        :param exclude_item_booking_from_revenue_accounting: The exclude_item_booking_from_revenue_accounting of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: bool
        """

        self._exclude_item_booking_from_revenue_accounting = exclude_item_booking_from_revenue_accounting

    @property
    def inline_discount_per_unit(self):
        """Gets the inline_discount_per_unit of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        This field is used in accordance with the `inlineDiscountType` field, in the following manner: * If the `inlineDiscountType` field is set as `Percentage`, this field specifies the discount percentage for each unit of the order line item. For exmaple, if you specify `5` in this field, the discount percentage is 5%. * If the `inlineDiscountType` field is set as `FixedAmount`, this field specifies the discount amount on each unit of the order line item. For exmaple, if you specify `10` in this field, the discount amount on each unit of the order line item is 10.  Once you set the `inlineDiscountType`, `inlineDiscountPerUnit`, and `listPricePerUnit` fields, the system will automatically generate the `amountPerUnit` field. You shall not set the `amountPerUnit` field by yourself.   # noqa: E501

        :return: The inline_discount_per_unit of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: float
        """
        return self._inline_discount_per_unit

    @inline_discount_per_unit.setter
    def inline_discount_per_unit(self, inline_discount_per_unit):
        """Sets the inline_discount_per_unit of this OrderLineItemCommonRetrieveOrderLineItem.

        This field is used in accordance with the `inlineDiscountType` field, in the following manner: * If the `inlineDiscountType` field is set as `Percentage`, this field specifies the discount percentage for each unit of the order line item. For exmaple, if you specify `5` in this field, the discount percentage is 5%. * If the `inlineDiscountType` field is set as `FixedAmount`, this field specifies the discount amount on each unit of the order line item. For exmaple, if you specify `10` in this field, the discount amount on each unit of the order line item is 10.  Once you set the `inlineDiscountType`, `inlineDiscountPerUnit`, and `listPricePerUnit` fields, the system will automatically generate the `amountPerUnit` field. You shall not set the `amountPerUnit` field by yourself.   # noqa: E501

        :param inline_discount_per_unit: The inline_discount_per_unit of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: float
        """

        self._inline_discount_per_unit = inline_discount_per_unit

    @property
    def inline_discount_type(self):
        """Gets the inline_discount_type of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        This field is used to specify the inline discount type, which can be `Percentage`, `FixedAmount`, or `None`. The default value is `Percentage`.  This field is used together with the `inlineDiscountPerUnit` field to specify inline discounts for order line items. The inline discount is applied to the list price of an order line item.   Once you set the `inlineDiscountType`, `inlineDiscountPerUnit`, and `listPricePerUnit` fields, the system will automatically generate the `amountPerUnit` field. You shall not set the `amountPerUnit` field by yourself.   # noqa: E501

        :return: The inline_discount_type of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._inline_discount_type

    @inline_discount_type.setter
    def inline_discount_type(self, inline_discount_type):
        """Sets the inline_discount_type of this OrderLineItemCommonRetrieveOrderLineItem.

        This field is used to specify the inline discount type, which can be `Percentage`, `FixedAmount`, or `None`. The default value is `Percentage`.  This field is used together with the `inlineDiscountPerUnit` field to specify inline discounts for order line items. The inline discount is applied to the list price of an order line item.   Once you set the `inlineDiscountType`, `inlineDiscountPerUnit`, and `listPricePerUnit` fields, the system will automatically generate the `amountPerUnit` field. You shall not set the `amountPerUnit` field by yourself.   # noqa: E501

        :param inline_discount_type: The inline_discount_type of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """
        allowed_values = ["Percentage", "FixedAmount", "None"]  # noqa: E501
        if (self._configuration.client_side_validation and
                inline_discount_type not in allowed_values):
            raise ValueError(
                "Invalid value for `inline_discount_type` ({0}), must be one of {1}"  # noqa: E501
                .format(inline_discount_type, allowed_values)
            )

        self._inline_discount_type = inline_discount_type

    @property
    def invoice_owner_account_id(self):
        """Gets the invoice_owner_account_id of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The account ID of the invoice owner of the order line item.   # noqa: E501

        :return: The invoice_owner_account_id of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._invoice_owner_account_id

    @invoice_owner_account_id.setter
    def invoice_owner_account_id(self, invoice_owner_account_id):
        """Sets the invoice_owner_account_id of this OrderLineItemCommonRetrieveOrderLineItem.

        The account ID of the invoice owner of the order line item.   # noqa: E501

        :param invoice_owner_account_id: The invoice_owner_account_id of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._invoice_owner_account_id = invoice_owner_account_id

    @property
    def invoice_owner_account_name(self):
        """Gets the invoice_owner_account_name of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The account name of the invoice owner of the order line item.   # noqa: E501

        :return: The invoice_owner_account_name of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._invoice_owner_account_name

    @invoice_owner_account_name.setter
    def invoice_owner_account_name(self, invoice_owner_account_name):
        """Sets the invoice_owner_account_name of this OrderLineItemCommonRetrieveOrderLineItem.

        The account name of the invoice owner of the order line item.   # noqa: E501

        :param invoice_owner_account_name: The invoice_owner_account_name of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._invoice_owner_account_name = invoice_owner_account_name

    @property
    def invoice_owner_account_number(self):
        """Gets the invoice_owner_account_number of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The account number of the invoice owner of the order line item.   # noqa: E501

        :return: The invoice_owner_account_number of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._invoice_owner_account_number

    @invoice_owner_account_number.setter
    def invoice_owner_account_number(self, invoice_owner_account_number):
        """Sets the invoice_owner_account_number of this OrderLineItemCommonRetrieveOrderLineItem.

        The account number of the invoice owner of the order line item.   # noqa: E501

        :param invoice_owner_account_number: The invoice_owner_account_number of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._invoice_owner_account_number = invoice_owner_account_number

    @property
    def is_allocation_eligible(self):
        """Gets the is_allocation_eligible of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        This field is used to identify if the charge segment is allocation eligible in revenue recognition.  **Note**: This feature is in the **Early Adopter** phase. If you want to use the feature, submit a request at <a href=\"https://support.zuora.com/\" target=\"_blank\">Zuora Global Support</a>, and we will evaluate whether the feature is suitable for your use cases.   # noqa: E501

        :return: The is_allocation_eligible of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: bool
        """
        return self._is_allocation_eligible

    @is_allocation_eligible.setter
    def is_allocation_eligible(self, is_allocation_eligible):
        """Sets the is_allocation_eligible of this OrderLineItemCommonRetrieveOrderLineItem.

        This field is used to identify if the charge segment is allocation eligible in revenue recognition.  **Note**: This feature is in the **Early Adopter** phase. If you want to use the feature, submit a request at <a href=\"https://support.zuora.com/\" target=\"_blank\">Zuora Global Support</a>, and we will evaluate whether the feature is suitable for your use cases.   # noqa: E501

        :param is_allocation_eligible: The is_allocation_eligible of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: bool
        """

        self._is_allocation_eligible = is_allocation_eligible

    @property
    def is_unbilled(self):
        """Gets the is_unbilled of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        This field is used to dictate how to perform the accounting during revenue recognition.  **Note**: This feature is in the **Early Adopter** phase. If you want to use the feature, submit a request at <a href=\"https://support.zuora.com/\" target=\"_blank\">Zuora Global Support</a>, and we will evaluate whether the feature is suitable for your use cases.   # noqa: E501

        :return: The is_unbilled of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: bool
        """
        return self._is_unbilled

    @is_unbilled.setter
    def is_unbilled(self, is_unbilled):
        """Sets the is_unbilled of this OrderLineItemCommonRetrieveOrderLineItem.

        This field is used to dictate how to perform the accounting during revenue recognition.  **Note**: This feature is in the **Early Adopter** phase. If you want to use the feature, submit a request at <a href=\"https://support.zuora.com/\" target=\"_blank\">Zuora Global Support</a>, and we will evaluate whether the feature is suitable for your use cases.   # noqa: E501

        :param is_unbilled: The is_unbilled of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: bool
        """

        self._is_unbilled = is_unbilled

    @property
    def item_category(self):
        """Gets the item_category of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The category for the Order Line Item, to indicate a product sales or return.   # noqa: E501

        :return: The item_category of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._item_category

    @item_category.setter
    def item_category(self, item_category):
        """Sets the item_category of this OrderLineItemCommonRetrieveOrderLineItem.

        The category for the Order Line Item, to indicate a product sales or return.   # noqa: E501

        :param item_category: The item_category of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """
        allowed_values = ["Sales", "Return"]  # noqa: E501
        if (self._configuration.client_side_validation and
                item_category not in allowed_values):
            raise ValueError(
                "Invalid value for `item_category` ({0}), must be one of {1}"  # noqa: E501
                .format(item_category, allowed_values)
            )

        self._item_category = item_category

    @property
    def item_name(self):
        """Gets the item_name of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The name of the Order Line Item.   # noqa: E501

        :return: The item_name of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._item_name

    @item_name.setter
    def item_name(self, item_name):
        """Sets the item_name of this OrderLineItemCommonRetrieveOrderLineItem.

        The name of the Order Line Item.   # noqa: E501

        :param item_name: The item_name of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._item_name = item_name

    @property
    def item_number(self):
        """Gets the item_number of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The number for the Order Line Item.   # noqa: E501

        :return: The item_number of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._item_number

    @item_number.setter
    def item_number(self, item_number):
        """Sets the item_number of this OrderLineItemCommonRetrieveOrderLineItem.

        The number for the Order Line Item.   # noqa: E501

        :param item_number: The item_number of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._item_number = item_number

    @property
    def item_state(self):
        """Gets the item_state of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The state of the Order Line Item. See [State transitions for an order, order line item, and fulfillment](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/Order_Line_Items/AB_Order_Line_Item_States_and_Order_States) for more information.   # noqa: E501

        :return: The item_state of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._item_state

    @item_state.setter
    def item_state(self, item_state):
        """Sets the item_state of this OrderLineItemCommonRetrieveOrderLineItem.

        The state of the Order Line Item. See [State transitions for an order, order line item, and fulfillment](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/Order_Line_Items/AB_Order_Line_Item_States_and_Order_States) for more information.   # noqa: E501

        :param item_state: The item_state of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """
        allowed_values = ["Executing", "Booked", "SentToBilling", "Complete", "Cancelled"]  # noqa: E501
        if (self._configuration.client_side_validation and
                item_state not in allowed_values):
            raise ValueError(
                "Invalid value for `item_state` ({0}), must be one of {1}"  # noqa: E501
                .format(item_state, allowed_values)
            )

        self._item_state = item_state

    @property
    def item_type(self):
        """Gets the item_type of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The type of the Order Line Item.    # noqa: E501

        :return: The item_type of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._item_type

    @item_type.setter
    def item_type(self, item_type):
        """Sets the item_type of this OrderLineItemCommonRetrieveOrderLineItem.

        The type of the Order Line Item.    # noqa: E501

        :param item_type: The item_type of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """
        allowed_values = ["Product", "Fee", "Services"]  # noqa: E501
        if (self._configuration.client_side_validation and
                item_type not in allowed_values):
            raise ValueError(
                "Invalid value for `item_type` ({0}), must be one of {1}"  # noqa: E501
                .format(item_type, allowed_values)
            )

        self._item_type = item_type

    @property
    def list_price(self):
        """Gets the list_price of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The extended list price for an order line item, calculated by the formula: listPrice = listPricePerUnit * quantity   # noqa: E501

        :return: The list_price of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: float
        """
        return self._list_price

    @list_price.setter
    def list_price(self, list_price):
        """Sets the list_price of this OrderLineItemCommonRetrieveOrderLineItem.

        The extended list price for an order line item, calculated by the formula: listPrice = listPricePerUnit * quantity   # noqa: E501

        :param list_price: The list_price of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: float
        """

        self._list_price = list_price

    @property
    def list_price_per_unit(self):
        """Gets the list_price_per_unit of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The list price per unit for the Order Line Item.   # noqa: E501

        :return: The list_price_per_unit of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: float
        """
        return self._list_price_per_unit

    @list_price_per_unit.setter
    def list_price_per_unit(self, list_price_per_unit):
        """Sets the list_price_per_unit of this OrderLineItemCommonRetrieveOrderLineItem.

        The list price per unit for the Order Line Item.   # noqa: E501

        :param list_price_per_unit: The list_price_per_unit of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: float
        """

        self._list_price_per_unit = list_price_per_unit

    @property
    def original_order_date(self):
        """Gets the original_order_date of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The date when the rate plan charge is created through an order or amendment. This field is to standardize the booking date information to increase audit ability and traceability of data between Zuora Billing and Zuora Revenue. It is mapped as the booking date for a sales order line in Zuora Revenue.   # noqa: E501

        :return: The original_order_date of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: date
        """
        return self._original_order_date

    @original_order_date.setter
    def original_order_date(self, original_order_date):
        """Sets the original_order_date of this OrderLineItemCommonRetrieveOrderLineItem.

        The date when the rate plan charge is created through an order or amendment. This field is to standardize the booking date information to increase audit ability and traceability of data between Zuora Billing and Zuora Revenue. It is mapped as the booking date for a sales order line in Zuora Revenue.   # noqa: E501

        :param original_order_date: The original_order_date of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: date
        """

        self._original_order_date = original_order_date

    @property
    def original_order_id(self):
        """Gets the original_order_id of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The ID of the original sales order for a return order line item.   # noqa: E501

        :return: The original_order_id of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._original_order_id

    @original_order_id.setter
    def original_order_id(self, original_order_id):
        """Sets the original_order_id of this OrderLineItemCommonRetrieveOrderLineItem.

        The ID of the original sales order for a return order line item.   # noqa: E501

        :param original_order_id: The original_order_id of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._original_order_id = original_order_id

    @property
    def original_order_line_item_id(self):
        """Gets the original_order_line_item_id of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The ID of the original sales order line item for a return order line item.   # noqa: E501

        :return: The original_order_line_item_id of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._original_order_line_item_id

    @original_order_line_item_id.setter
    def original_order_line_item_id(self, original_order_line_item_id):
        """Sets the original_order_line_item_id of this OrderLineItemCommonRetrieveOrderLineItem.

        The ID of the original sales order line item for a return order line item.   # noqa: E501

        :param original_order_line_item_id: The original_order_line_item_id of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._original_order_line_item_id = original_order_line_item_id

    @property
    def original_order_line_item_number(self):
        """Gets the original_order_line_item_number of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The number of the original sales order line item for a return order line item.   # noqa: E501

        :return: The original_order_line_item_number of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._original_order_line_item_number

    @original_order_line_item_number.setter
    def original_order_line_item_number(self, original_order_line_item_number):
        """Sets the original_order_line_item_number of this OrderLineItemCommonRetrieveOrderLineItem.

        The number of the original sales order line item for a return order line item.   # noqa: E501

        :param original_order_line_item_number: The original_order_line_item_number of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._original_order_line_item_number = original_order_line_item_number

    @property
    def original_order_number(self):
        """Gets the original_order_number of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The number of the original sales order for a return order line item.   # noqa: E501

        :return: The original_order_number of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._original_order_number

    @original_order_number.setter
    def original_order_number(self, original_order_number):
        """Sets the original_order_number of this OrderLineItemCommonRetrieveOrderLineItem.

        The number of the original sales order for a return order line item.   # noqa: E501

        :param original_order_number: The original_order_number of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._original_order_number = original_order_number

    @property
    def owner_account_id(self):
        """Gets the owner_account_id of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The account ID of the owner of the order line item.   # noqa: E501

        :return: The owner_account_id of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._owner_account_id

    @owner_account_id.setter
    def owner_account_id(self, owner_account_id):
        """Sets the owner_account_id of this OrderLineItemCommonRetrieveOrderLineItem.

        The account ID of the owner of the order line item.   # noqa: E501

        :param owner_account_id: The owner_account_id of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._owner_account_id = owner_account_id

    @property
    def owner_account_name(self):
        """Gets the owner_account_name of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The account name of the owner of the order line item.   # noqa: E501

        :return: The owner_account_name of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._owner_account_name

    @owner_account_name.setter
    def owner_account_name(self, owner_account_name):
        """Sets the owner_account_name of this OrderLineItemCommonRetrieveOrderLineItem.

        The account name of the owner of the order line item.   # noqa: E501

        :param owner_account_name: The owner_account_name of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._owner_account_name = owner_account_name

    @property
    def owner_account_number(self):
        """Gets the owner_account_number of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The account number of the owner of the order line item.   # noqa: E501

        :return: The owner_account_number of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._owner_account_number

    @owner_account_number.setter
    def owner_account_number(self, owner_account_number):
        """Sets the owner_account_number of this OrderLineItemCommonRetrieveOrderLineItem.

        The account number of the owner of the order line item.   # noqa: E501

        :param owner_account_number: The owner_account_number of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._owner_account_number = owner_account_number

    @property
    def product_code(self):
        """Gets the product_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The product code for the Order Line Item.   # noqa: E501

        :return: The product_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._product_code

    @product_code.setter
    def product_code(self, product_code):
        """Sets the product_code of this OrderLineItemCommonRetrieveOrderLineItem.

        The product code for the Order Line Item.   # noqa: E501

        :param product_code: The product_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._product_code = product_code

    @property
    def product_rate_plan_charge_id(self):
        """Gets the product_rate_plan_charge_id of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        Id of a Product Rate Plan Charge. Only one-time charges are supported.   # noqa: E501

        :return: The product_rate_plan_charge_id of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._product_rate_plan_charge_id

    @product_rate_plan_charge_id.setter
    def product_rate_plan_charge_id(self, product_rate_plan_charge_id):
        """Sets the product_rate_plan_charge_id of this OrderLineItemCommonRetrieveOrderLineItem.

        Id of a Product Rate Plan Charge. Only one-time charges are supported.   # noqa: E501

        :param product_rate_plan_charge_id: The product_rate_plan_charge_id of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._product_rate_plan_charge_id = product_rate_plan_charge_id

    @property
    def purchase_order_number(self):
        """Gets the purchase_order_number of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        Used by customers to specify the Purchase Order Number provided by the buyer.   # noqa: E501

        :return: The purchase_order_number of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._purchase_order_number

    @purchase_order_number.setter
    def purchase_order_number(self, purchase_order_number):
        """Sets the purchase_order_number of this OrderLineItemCommonRetrieveOrderLineItem.

        Used by customers to specify the Purchase Order Number provided by the buyer.   # noqa: E501

        :param purchase_order_number: The purchase_order_number of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._purchase_order_number = purchase_order_number

    @property
    def quantity(self):
        """Gets the quantity of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The quantity of units, such as the number of authors in a hosted wiki service.   # noqa: E501

        :return: The quantity of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: float
        """
        return self._quantity

    @quantity.setter
    def quantity(self, quantity):
        """Sets the quantity of this OrderLineItemCommonRetrieveOrderLineItem.

        The quantity of units, such as the number of authors in a hosted wiki service.   # noqa: E501

        :param quantity: The quantity of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: float
        """

        self._quantity = quantity

    @property
    def quantity_available_for_return(self):
        """Gets the quantity_available_for_return of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The quantity that can be returned for an order line item.    # noqa: E501

        :return: The quantity_available_for_return of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: float
        """
        return self._quantity_available_for_return

    @quantity_available_for_return.setter
    def quantity_available_for_return(self, quantity_available_for_return):
        """Sets the quantity_available_for_return of this OrderLineItemCommonRetrieveOrderLineItem.

        The quantity that can be returned for an order line item.    # noqa: E501

        :param quantity_available_for_return: The quantity_available_for_return of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: float
        """

        self._quantity_available_for_return = quantity_available_for_return

    @property
    def quantity_fulfilled(self):
        """Gets the quantity_fulfilled of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The fulfilled quantity for an order line item.    # noqa: E501

        :return: The quantity_fulfilled of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: float
        """
        return self._quantity_fulfilled

    @quantity_fulfilled.setter
    def quantity_fulfilled(self, quantity_fulfilled):
        """Sets the quantity_fulfilled of this OrderLineItemCommonRetrieveOrderLineItem.

        The fulfilled quantity for an order line item.    # noqa: E501

        :param quantity_fulfilled: The quantity_fulfilled of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: float
        """

        self._quantity_fulfilled = quantity_fulfilled

    @property
    def quantity_pending_fulfillment(self):
        """Gets the quantity_pending_fulfillment of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The quantity to fulfill for an order line item.    # noqa: E501

        :return: The quantity_pending_fulfillment of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: float
        """
        return self._quantity_pending_fulfillment

    @quantity_pending_fulfillment.setter
    def quantity_pending_fulfillment(self, quantity_pending_fulfillment):
        """Sets the quantity_pending_fulfillment of this OrderLineItemCommonRetrieveOrderLineItem.

        The quantity to fulfill for an order line item.    # noqa: E501

        :param quantity_pending_fulfillment: The quantity_pending_fulfillment of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: float
        """

        self._quantity_pending_fulfillment = quantity_pending_fulfillment

    @property
    def recognized_revenue_accounting_code(self):
        """Gets the recognized_revenue_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The recognized revenue accounting code for the Order Line Item.   # noqa: E501

        :return: The recognized_revenue_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._recognized_revenue_accounting_code

    @recognized_revenue_accounting_code.setter
    def recognized_revenue_accounting_code(self, recognized_revenue_accounting_code):
        """Sets the recognized_revenue_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.

        The recognized revenue accounting code for the Order Line Item.   # noqa: E501

        :param recognized_revenue_accounting_code: The recognized_revenue_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._recognized_revenue_accounting_code = recognized_revenue_accounting_code

    @property
    def related_subscription_number(self):
        """Gets the related_subscription_number of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        Use this field to relate an order line item to an subscription. Specify this field to the subscription number of the subscription to relate.   # noqa: E501

        :return: The related_subscription_number of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._related_subscription_number

    @related_subscription_number.setter
    def related_subscription_number(self, related_subscription_number):
        """Sets the related_subscription_number of this OrderLineItemCommonRetrieveOrderLineItem.

        Use this field to relate an order line item to an subscription. Specify this field to the subscription number of the subscription to relate.   # noqa: E501

        :param related_subscription_number: The related_subscription_number of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._related_subscription_number = related_subscription_number

    @property
    def requires_fulfillment(self):
        """Gets the requires_fulfillment of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The flag to show whether fulfillment is needed or not. It's derived from billing rule of the Order Line Item.   # noqa: E501

        :return: The requires_fulfillment of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: bool
        """
        return self._requires_fulfillment

    @requires_fulfillment.setter
    def requires_fulfillment(self, requires_fulfillment):
        """Sets the requires_fulfillment of this OrderLineItemCommonRetrieveOrderLineItem.

        The flag to show whether fulfillment is needed or not. It's derived from billing rule of the Order Line Item.   # noqa: E501

        :param requires_fulfillment: The requires_fulfillment of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: bool
        """

        self._requires_fulfillment = requires_fulfillment

    @property
    def revenue_recognition_rule(self):
        """Gets the revenue_recognition_rule of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The Revenue Recognition rule for the Order Line Item.   # noqa: E501

        :return: The revenue_recognition_rule of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._revenue_recognition_rule

    @revenue_recognition_rule.setter
    def revenue_recognition_rule(self, revenue_recognition_rule):
        """Sets the revenue_recognition_rule of this OrderLineItemCommonRetrieveOrderLineItem.

        The Revenue Recognition rule for the Order Line Item.   # noqa: E501

        :param revenue_recognition_rule: The revenue_recognition_rule of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._revenue_recognition_rule = revenue_recognition_rule

    @property
    def sequence_set_id(self):
        """Gets the sequence_set_id of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The ID of the sequence set associated with the OrderLineItem.    **Note**: If you have the [Flexible Billing Attributes](https://knowledgecenter.zuora.com/Billing/Subscriptions/Flexible_Billing_Attributes) feature disabled, this field is unavailable in the request body and the value of this field is `null` in the response body.   # noqa: E501

        :return: The sequence_set_id of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._sequence_set_id

    @sequence_set_id.setter
    def sequence_set_id(self, sequence_set_id):
        """Sets the sequence_set_id of this OrderLineItemCommonRetrieveOrderLineItem.

        The ID of the sequence set associated with the OrderLineItem.    **Note**: If you have the [Flexible Billing Attributes](https://knowledgecenter.zuora.com/Billing/Subscriptions/Flexible_Billing_Attributes) feature disabled, this field is unavailable in the request body and the value of this field is `null` in the response body.   # noqa: E501

        :param sequence_set_id: The sequence_set_id of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._sequence_set_id = sequence_set_id

    @property
    def sold_to(self):
        """Gets the sold_to of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The ID of a contact that belongs to the owner account or billing account of the order line item. Use this field to assign an existing account as the sold-to contact of an order line item.   # noqa: E501

        :return: The sold_to of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._sold_to

    @sold_to.setter
    def sold_to(self, sold_to):
        """Sets the sold_to of this OrderLineItemCommonRetrieveOrderLineItem.

        The ID of a contact that belongs to the owner account or billing account of the order line item. Use this field to assign an existing account as the sold-to contact of an order line item.   # noqa: E501

        :param sold_to: The sold_to of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._sold_to = sold_to

    @property
    def sold_to_snapshot_id(self):
        """Gets the sold_to_snapshot_id of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The snapshot of the ID for an account used as the sold-to contact of an order line item. This field is used to store the original information about the account, in case the information about the account is changed after the creation of the order line item. The `soldToSnapshotId` field is exposed while retrieving the order line item details.   # noqa: E501

        :return: The sold_to_snapshot_id of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._sold_to_snapshot_id

    @sold_to_snapshot_id.setter
    def sold_to_snapshot_id(self, sold_to_snapshot_id):
        """Sets the sold_to_snapshot_id of this OrderLineItemCommonRetrieveOrderLineItem.

        The snapshot of the ID for an account used as the sold-to contact of an order line item. This field is used to store the original information about the account, in case the information about the account is changed after the creation of the order line item. The `soldToSnapshotId` field is exposed while retrieving the order line item details.   # noqa: E501

        :param sold_to_snapshot_id: The sold_to_snapshot_id of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._sold_to_snapshot_id = sold_to_snapshot_id

    @property
    def tax_code(self):
        """Gets the tax_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The tax code for the Order Line Item.   # noqa: E501

        :return: The tax_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._tax_code

    @tax_code.setter
    def tax_code(self, tax_code):
        """Sets the tax_code of this OrderLineItemCommonRetrieveOrderLineItem.

        The tax code for the Order Line Item.   # noqa: E501

        :param tax_code: The tax_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._tax_code = tax_code

    @property
    def tax_mode(self):
        """Gets the tax_mode of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The tax mode for the Order Line Item.   # noqa: E501

        :return: The tax_mode of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._tax_mode

    @tax_mode.setter
    def tax_mode(self, tax_mode):
        """Sets the tax_mode of this OrderLineItemCommonRetrieveOrderLineItem.

        The tax mode for the Order Line Item.   # noqa: E501

        :param tax_mode: The tax_mode of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """
        allowed_values = ["TaxInclusive", "TaxExclusive"]  # noqa: E501
        if (self._configuration.client_side_validation and
                tax_mode not in allowed_values):
            raise ValueError(
                "Invalid value for `tax_mode` ({0}), must be one of {1}"  # noqa: E501
                .format(tax_mode, allowed_values)
            )

        self._tax_mode = tax_mode

    @property
    def transaction_end_date(self):
        """Gets the transaction_end_date of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The date a transaction is completed. The default value of this field is the transaction start date. Also, the value of this field should always equal or be later than the value of the `transactionStartDate` field.   # noqa: E501

        :return: The transaction_end_date of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: date
        """
        return self._transaction_end_date

    @transaction_end_date.setter
    def transaction_end_date(self, transaction_end_date):
        """Sets the transaction_end_date of this OrderLineItemCommonRetrieveOrderLineItem.

        The date a transaction is completed. The default value of this field is the transaction start date. Also, the value of this field should always equal or be later than the value of the `transactionStartDate` field.   # noqa: E501

        :param transaction_end_date: The transaction_end_date of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: date
        """

        self._transaction_end_date = transaction_end_date

    @property
    def transaction_start_date(self):
        """Gets the transaction_start_date of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The date a transaction starts. The default value of this field is the order date.   # noqa: E501

        :return: The transaction_start_date of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: date
        """
        return self._transaction_start_date

    @transaction_start_date.setter
    def transaction_start_date(self, transaction_start_date):
        """Sets the transaction_start_date of this OrderLineItemCommonRetrieveOrderLineItem.

        The date a transaction starts. The default value of this field is the order date.   # noqa: E501

        :param transaction_start_date: The transaction_start_date of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: date
        """

        self._transaction_start_date = transaction_start_date

    @property
    def unbilled_receivables_accounting_code(self):
        """Gets the unbilled_receivables_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501

        The accounting code on the Order Line Item object for customers using [Zuora Billing - Revenue Integration](https://knowledgecenter.zuora.com/Zuora_Revenue/Zuora_Billing_-_Revenue_Integration).   # noqa: E501

        :return: The unbilled_receivables_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :rtype: str
        """
        return self._unbilled_receivables_accounting_code

    @unbilled_receivables_accounting_code.setter
    def unbilled_receivables_accounting_code(self, unbilled_receivables_accounting_code):
        """Sets the unbilled_receivables_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.

        The accounting code on the Order Line Item object for customers using [Zuora Billing - Revenue Integration](https://knowledgecenter.zuora.com/Zuora_Revenue/Zuora_Billing_-_Revenue_Integration).   # noqa: E501

        :param unbilled_receivables_accounting_code: The unbilled_receivables_accounting_code of this OrderLineItemCommonRetrieveOrderLineItem.  # noqa: E501
        :type: str
        """

        self._unbilled_receivables_accounting_code = unbilled_receivables_accounting_code

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
        if issubclass(OrderLineItemCommonRetrieveOrderLineItem, dict):
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
        if not isinstance(other, OrderLineItemCommonRetrieveOrderLineItem):
            return False

        return self.to_dict() == other.to_dict()

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        if not isinstance(other, OrderLineItemCommonRetrieveOrderLineItem):
            return True

        return self.to_dict() != other.to_dict()
