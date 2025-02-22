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


class POSTAccountTypePaymentMethod(object):
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
        'baid': 'str',
        'email': 'str',
        'preapproval_key': 'str',
        'card_holder_info': 'CreatePaymentMethodCardholderInfo',
        'card_number': 'str',
        'card_type': 'str',
        'check_duplicated': 'bool',
        'expiration_month': 'int',
        'expiration_year': 'int',
        'mit_consent_agreement_ref': 'str',
        'mit_consent_agreement_src': 'str',
        'mit_network_transaction_id': 'str',
        'mit_profile_action': 'str',
        'mit_profile_agreed_on': 'date',
        'mit_profile_type': 'str',
        'security_code': 'str',
        'address_line1': 'str',
        'address_line2': 'str',
        'bank_aba_code': 'str',
        'bank_account_name': 'str',
        'bank_account_number': 'str',
        'bank_account_type': 'str',
        'bank_name': 'str',
        'city': 'str',
        'country': 'str',
        'phone': 'str',
        'state': 'str',
        'zip_code': 'str',
        'account_key': 'str',
        'auth_gateway': 'str',
        'ip_address': 'str',
        'make_default': 'bool',
        'iban': 'str',
        'account_holder_info': 'CreatePaymentMethodBankTransferAccountHolderInfo',
        'account_number': 'str',
        'bank_code': 'str',
        'branch_code': 'str',
        'business_identification_code': 'str',
        'currency_code': 'str',
        'identity_number': 'str',
        'credit_card_mask_number': 'str',
        'second_token_id': 'str',
        'token_id': 'str',
        'apple_payment_data': 'str',
        'google_payment_token': 'str'
    }

    attribute_map = {
        'baid': 'BAID',
        'email': 'email',
        'preapproval_key': 'preapprovalKey',
        'card_holder_info': 'cardHolderInfo',
        'card_number': 'cardNumber',
        'card_type': 'cardType',
        'check_duplicated': 'checkDuplicated',
        'expiration_month': 'expirationMonth',
        'expiration_year': 'expirationYear',
        'mit_consent_agreement_ref': 'mitConsentAgreementRef',
        'mit_consent_agreement_src': 'mitConsentAgreementSrc',
        'mit_network_transaction_id': 'mitNetworkTransactionId',
        'mit_profile_action': 'mitProfileAction',
        'mit_profile_agreed_on': 'mitProfileAgreedOn',
        'mit_profile_type': 'mitProfileType',
        'security_code': 'securityCode',
        'address_line1': 'addressLine1',
        'address_line2': 'addressLine2',
        'bank_aba_code': 'bankABACode',
        'bank_account_name': 'bankAccountName',
        'bank_account_number': 'bankAccountNumber',
        'bank_account_type': 'bankAccountType',
        'bank_name': 'bankName',
        'city': 'city',
        'country': 'country',
        'phone': 'phone',
        'state': 'state',
        'zip_code': 'zipCode',
        'account_key': 'accountKey',
        'auth_gateway': 'authGateway',
        'ip_address': 'ipAddress',
        'make_default': 'makeDefault',
        'iban': 'IBAN',
        'account_holder_info': 'accountHolderInfo',
        'account_number': 'accountNumber',
        'bank_code': 'bankCode',
        'branch_code': 'branchCode',
        'business_identification_code': 'businessIdentificationCode',
        'currency_code': 'currencyCode',
        'identity_number': 'identityNumber',
        'credit_card_mask_number': 'creditCardMaskNumber',
        'second_token_id': 'secondTokenId',
        'token_id': 'tokenId',
        'apple_payment_data': 'applePaymentData',
        'google_payment_token': 'googlePaymentToken'
    }

    def __init__(self, baid=None, email=None, preapproval_key=None, card_holder_info=None, card_number=None, card_type=None, check_duplicated=None, expiration_month=None, expiration_year=None, mit_consent_agreement_ref=None, mit_consent_agreement_src=None, mit_network_transaction_id=None, mit_profile_action=None, mit_profile_agreed_on=None, mit_profile_type=None, security_code=None, address_line1=None, address_line2=None, bank_aba_code=None, bank_account_name=None, bank_account_number=None, bank_account_type=None, bank_name=None, city=None, country=None, phone=None, state=None, zip_code=None, account_key=None, auth_gateway=None, ip_address=None, make_default=False, iban=None, account_holder_info=None, account_number=None, bank_code=None, branch_code=None, business_identification_code=None, currency_code=None, identity_number=None, credit_card_mask_number=None, second_token_id=None, token_id=None, apple_payment_data=None, google_payment_token=None, _configuration=None):  # noqa: E501
        """POSTAccountTypePaymentMethod - a model defined in Swagger"""  # noqa: E501
        if _configuration is None:
            _configuration = Configuration()
        self._configuration = _configuration

        self._baid = None
        self._email = None
        self._preapproval_key = None
        self._card_holder_info = None
        self._card_number = None
        self._card_type = None
        self._check_duplicated = None
        self._expiration_month = None
        self._expiration_year = None
        self._mit_consent_agreement_ref = None
        self._mit_consent_agreement_src = None
        self._mit_network_transaction_id = None
        self._mit_profile_action = None
        self._mit_profile_agreed_on = None
        self._mit_profile_type = None
        self._security_code = None
        self._address_line1 = None
        self._address_line2 = None
        self._bank_aba_code = None
        self._bank_account_name = None
        self._bank_account_number = None
        self._bank_account_type = None
        self._bank_name = None
        self._city = None
        self._country = None
        self._phone = None
        self._state = None
        self._zip_code = None
        self._account_key = None
        self._auth_gateway = None
        self._ip_address = None
        self._make_default = None
        self._iban = None
        self._account_holder_info = None
        self._account_number = None
        self._bank_code = None
        self._branch_code = None
        self._business_identification_code = None
        self._currency_code = None
        self._identity_number = None
        self._credit_card_mask_number = None
        self._second_token_id = None
        self._token_id = None
        self._apple_payment_data = None
        self._google_payment_token = None
        self.discriminator = None

        if baid is not None:
            self.baid = baid
        if email is not None:
            self.email = email
        if preapproval_key is not None:
            self.preapproval_key = preapproval_key
        if card_holder_info is not None:
            self.card_holder_info = card_holder_info
        if card_number is not None:
            self.card_number = card_number
        if card_type is not None:
            self.card_type = card_type
        if check_duplicated is not None:
            self.check_duplicated = check_duplicated
        if expiration_month is not None:
            self.expiration_month = expiration_month
        if expiration_year is not None:
            self.expiration_year = expiration_year
        if mit_consent_agreement_ref is not None:
            self.mit_consent_agreement_ref = mit_consent_agreement_ref
        if mit_consent_agreement_src is not None:
            self.mit_consent_agreement_src = mit_consent_agreement_src
        if mit_network_transaction_id is not None:
            self.mit_network_transaction_id = mit_network_transaction_id
        if mit_profile_action is not None:
            self.mit_profile_action = mit_profile_action
        if mit_profile_agreed_on is not None:
            self.mit_profile_agreed_on = mit_profile_agreed_on
        if mit_profile_type is not None:
            self.mit_profile_type = mit_profile_type
        if security_code is not None:
            self.security_code = security_code
        if address_line1 is not None:
            self.address_line1 = address_line1
        if address_line2 is not None:
            self.address_line2 = address_line2
        if bank_aba_code is not None:
            self.bank_aba_code = bank_aba_code
        if bank_account_name is not None:
            self.bank_account_name = bank_account_name
        if bank_account_number is not None:
            self.bank_account_number = bank_account_number
        if bank_account_type is not None:
            self.bank_account_type = bank_account_type
        if bank_name is not None:
            self.bank_name = bank_name
        if city is not None:
            self.city = city
        if country is not None:
            self.country = country
        if phone is not None:
            self.phone = phone
        if state is not None:
            self.state = state
        if zip_code is not None:
            self.zip_code = zip_code
        if account_key is not None:
            self.account_key = account_key
        if auth_gateway is not None:
            self.auth_gateway = auth_gateway
        if ip_address is not None:
            self.ip_address = ip_address
        if make_default is not None:
            self.make_default = make_default
        if iban is not None:
            self.iban = iban
        if account_holder_info is not None:
            self.account_holder_info = account_holder_info
        if account_number is not None:
            self.account_number = account_number
        if bank_code is not None:
            self.bank_code = bank_code
        if branch_code is not None:
            self.branch_code = branch_code
        if business_identification_code is not None:
            self.business_identification_code = business_identification_code
        if currency_code is not None:
            self.currency_code = currency_code
        if identity_number is not None:
            self.identity_number = identity_number
        if credit_card_mask_number is not None:
            self.credit_card_mask_number = credit_card_mask_number
        if second_token_id is not None:
            self.second_token_id = second_token_id
        if token_id is not None:
            self.token_id = token_id
        if apple_payment_data is not None:
            self.apple_payment_data = apple_payment_data
        if google_payment_token is not None:
            self.google_payment_token = google_payment_token

    @property
    def baid(self):
        """Gets the baid of this POSTAccountTypePaymentMethod.  # noqa: E501

        ID of a PayPal billing agreement. For example, I-1TJ3GAGG82Y9.   # noqa: E501

        :return: The baid of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._baid

    @baid.setter
    def baid(self, baid):
        """Sets the baid of this POSTAccountTypePaymentMethod.

        ID of a PayPal billing agreement. For example, I-1TJ3GAGG82Y9.   # noqa: E501

        :param baid: The baid of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._baid = baid

    @property
    def email(self):
        """Gets the email of this POSTAccountTypePaymentMethod.  # noqa: E501

        Email address associated with the payment method. This field is supported for the following payment methods:   - PayPal payment methods. This field is required for creating any of the following PayPal payment methods.     - PayPal Express Checkout     - PayPal Adaptive     - PayPal Commerce Platform   - Apple Pay and Google Pay payment methods on Adyen v2.0. This field will be passed to Adyen as `shopperEmail`.   # noqa: E501

        :return: The email of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._email

    @email.setter
    def email(self, email):
        """Sets the email of this POSTAccountTypePaymentMethod.

        Email address associated with the payment method. This field is supported for the following payment methods:   - PayPal payment methods. This field is required for creating any of the following PayPal payment methods.     - PayPal Express Checkout     - PayPal Adaptive     - PayPal Commerce Platform   - Apple Pay and Google Pay payment methods on Adyen v2.0. This field will be passed to Adyen as `shopperEmail`.   # noqa: E501

        :param email: The email of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._email = email

    @property
    def preapproval_key(self):
        """Gets the preapproval_key of this POSTAccountTypePaymentMethod.  # noqa: E501

        The PayPal preapproval key.   # noqa: E501

        :return: The preapproval_key of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._preapproval_key

    @preapproval_key.setter
    def preapproval_key(self, preapproval_key):
        """Sets the preapproval_key of this POSTAccountTypePaymentMethod.

        The PayPal preapproval key.   # noqa: E501

        :param preapproval_key: The preapproval_key of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._preapproval_key = preapproval_key

    @property
    def card_holder_info(self):
        """Gets the card_holder_info of this POSTAccountTypePaymentMethod.  # noqa: E501


        :return: The card_holder_info of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: CreatePaymentMethodCardholderInfo
        """
        return self._card_holder_info

    @card_holder_info.setter
    def card_holder_info(self, card_holder_info):
        """Sets the card_holder_info of this POSTAccountTypePaymentMethod.


        :param card_holder_info: The card_holder_info of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: CreatePaymentMethodCardholderInfo
        """

        self._card_holder_info = card_holder_info

    @property
    def card_number(self):
        """Gets the card_number of this POSTAccountTypePaymentMethod.  # noqa: E501

        Credit card number. This field is required if `type` is set to `CreditCard`.   # noqa: E501

        :return: The card_number of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._card_number

    @card_number.setter
    def card_number(self, card_number):
        """Sets the card_number of this POSTAccountTypePaymentMethod.

        Credit card number. This field is required if `type` is set to `CreditCard`.   # noqa: E501

        :param card_number: The card_number of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._card_number = card_number

    @property
    def card_type(self):
        """Gets the card_type of this POSTAccountTypePaymentMethod.  # noqa: E501

        The type of the credit card. This field is required if `type` is set to `CreditCard`.  Possible values include `Visa`, `MasterCard`, `AmericanExpress`, `Discover`, `JCB`, and `Diners`. For more information about credit card types supported by different payment gateways, see [Supported Payment Gateways](https://knowledgecenter.zuora.com/CB_Billing/M_Payment_Gateways/Supported_Payment_Gateways).   # noqa: E501

        :return: The card_type of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._card_type

    @card_type.setter
    def card_type(self, card_type):
        """Sets the card_type of this POSTAccountTypePaymentMethod.

        The type of the credit card. This field is required if `type` is set to `CreditCard`.  Possible values include `Visa`, `MasterCard`, `AmericanExpress`, `Discover`, `JCB`, and `Diners`. For more information about credit card types supported by different payment gateways, see [Supported Payment Gateways](https://knowledgecenter.zuora.com/CB_Billing/M_Payment_Gateways/Supported_Payment_Gateways).   # noqa: E501

        :param card_type: The card_type of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._card_type = card_type

    @property
    def check_duplicated(self):
        """Gets the check_duplicated of this POSTAccountTypePaymentMethod.  # noqa: E501

        Indicates whether the duplication check is performed when you create a new credit card payment method. The default value is `false`.  With this field set to `true`, Zuora will check all active payment methods associated with the same billing account to ensure that no duplicate credit card payment methods are created. An error is returned if a duplicate payment method is found.          The following fields are used for the duplication check:   - `cardHolderName`   - `expirationMonth`   - `expirationYear`   - `creditCardMaskNumber`. It is the masked credit card number generated by Zuora. For example:     ```     ************1234     ```   # noqa: E501

        :return: The check_duplicated of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: bool
        """
        return self._check_duplicated

    @check_duplicated.setter
    def check_duplicated(self, check_duplicated):
        """Sets the check_duplicated of this POSTAccountTypePaymentMethod.

        Indicates whether the duplication check is performed when you create a new credit card payment method. The default value is `false`.  With this field set to `true`, Zuora will check all active payment methods associated with the same billing account to ensure that no duplicate credit card payment methods are created. An error is returned if a duplicate payment method is found.          The following fields are used for the duplication check:   - `cardHolderName`   - `expirationMonth`   - `expirationYear`   - `creditCardMaskNumber`. It is the masked credit card number generated by Zuora. For example:     ```     ************1234     ```   # noqa: E501

        :param check_duplicated: The check_duplicated of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: bool
        """

        self._check_duplicated = check_duplicated

    @property
    def expiration_month(self):
        """Gets the expiration_month of this POSTAccountTypePaymentMethod.  # noqa: E501

        One or two digit expiration month (1-12) of the credit card. This field is required if `type` is set to `CreditCard`.   # noqa: E501

        :return: The expiration_month of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: int
        """
        return self._expiration_month

    @expiration_month.setter
    def expiration_month(self, expiration_month):
        """Sets the expiration_month of this POSTAccountTypePaymentMethod.

        One or two digit expiration month (1-12) of the credit card. This field is required if `type` is set to `CreditCard`.   # noqa: E501

        :param expiration_month: The expiration_month of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: int
        """

        self._expiration_month = expiration_month

    @property
    def expiration_year(self):
        """Gets the expiration_year of this POSTAccountTypePaymentMethod.  # noqa: E501

        Four-digit expiration year of the credit card. This field is required if `type` is set to `CreditCard`.   # noqa: E501

        :return: The expiration_year of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: int
        """
        return self._expiration_year

    @expiration_year.setter
    def expiration_year(self, expiration_year):
        """Sets the expiration_year of this POSTAccountTypePaymentMethod.

        Four-digit expiration year of the credit card. This field is required if `type` is set to `CreditCard`.   # noqa: E501

        :param expiration_year: The expiration_year of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: int
        """

        self._expiration_year = expiration_year

    @property
    def mit_consent_agreement_ref(self):
        """Gets the mit_consent_agreement_ref of this POSTAccountTypePaymentMethod.  # noqa: E501

        Specifies your reference for the stored credential consent agreement that you have established with the customer. Only applicable if you set the `mitProfileAction` field.   # noqa: E501

        :return: The mit_consent_agreement_ref of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._mit_consent_agreement_ref

    @mit_consent_agreement_ref.setter
    def mit_consent_agreement_ref(self, mit_consent_agreement_ref):
        """Sets the mit_consent_agreement_ref of this POSTAccountTypePaymentMethod.

        Specifies your reference for the stored credential consent agreement that you have established with the customer. Only applicable if you set the `mitProfileAction` field.   # noqa: E501

        :param mit_consent_agreement_ref: The mit_consent_agreement_ref of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """
        if (self._configuration.client_side_validation and
                mit_consent_agreement_ref is not None and len(mit_consent_agreement_ref) > 128):
            raise ValueError("Invalid value for `mit_consent_agreement_ref`, length must be less than or equal to `128`")  # noqa: E501

        self._mit_consent_agreement_ref = mit_consent_agreement_ref

    @property
    def mit_consent_agreement_src(self):
        """Gets the mit_consent_agreement_src of this POSTAccountTypePaymentMethod.  # noqa: E501

        Required if you set the `mitProfileAction` field. Specifies how the consent agreement has been established with the customer. The allowed value is `External`. If you do not specify the `mitProfileAction` field, Zuora will automatically create a stored credential profile for the payment method, with the default value `External` set to this field.   # noqa: E501

        :return: The mit_consent_agreement_src of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._mit_consent_agreement_src

    @mit_consent_agreement_src.setter
    def mit_consent_agreement_src(self, mit_consent_agreement_src):
        """Sets the mit_consent_agreement_src of this POSTAccountTypePaymentMethod.

        Required if you set the `mitProfileAction` field. Specifies how the consent agreement has been established with the customer. The allowed value is `External`. If you do not specify the `mitProfileAction` field, Zuora will automatically create a stored credential profile for the payment method, with the default value `External` set to this field.   # noqa: E501

        :param mit_consent_agreement_src: The mit_consent_agreement_src of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """
        allowed_values = ["External"]  # noqa: E501
        if (self._configuration.client_side_validation and
                mit_consent_agreement_src not in allowed_values):
            raise ValueError(
                "Invalid value for `mit_consent_agreement_src` ({0}), must be one of {1}"  # noqa: E501
                .format(mit_consent_agreement_src, allowed_values)
            )

        self._mit_consent_agreement_src = mit_consent_agreement_src

    @property
    def mit_network_transaction_id(self):
        """Gets the mit_network_transaction_id of this POSTAccountTypePaymentMethod.  # noqa: E501

        Specifies the ID of a network transaction. Only applicable if you set the `mitProfileAction` field to `Persist`.   # noqa: E501

        :return: The mit_network_transaction_id of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._mit_network_transaction_id

    @mit_network_transaction_id.setter
    def mit_network_transaction_id(self, mit_network_transaction_id):
        """Sets the mit_network_transaction_id of this POSTAccountTypePaymentMethod.

        Specifies the ID of a network transaction. Only applicable if you set the `mitProfileAction` field to `Persist`.   # noqa: E501

        :param mit_network_transaction_id: The mit_network_transaction_id of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """
        if (self._configuration.client_side_validation and
                mit_network_transaction_id is not None and len(mit_network_transaction_id) > 128):
            raise ValueError("Invalid value for `mit_network_transaction_id`, length must be less than or equal to `128`")  # noqa: E501

        self._mit_network_transaction_id = mit_network_transaction_id

    @property
    def mit_profile_action(self):
        """Gets the mit_profile_action of this POSTAccountTypePaymentMethod.  # noqa: E501

        Specifies how Zuora creates and activates the stored credential profile.  - `Activate` - Use this value if you are creating the stored credential profile after receiving the customer's consent.    Zuora will create the stored credential profile then send a cardholder-initiated transaction (CIT) to the payment gateway to validate the stored credential profile. If the CIT succeeds, the status of the stored credential profile will be `Active`. If the CIT does not succeed, Zuora will not create a stored credential profile.      If the payment gateway does not support the stored credential transaction framework, the status of the stored credential profile will be `Agreed`.   - `Persist` - Use this value if the stored credential profile represents a stored credential profile in an external system. The status of the payment method's stored credential profile will be `Active`.  If you do not specify this field, Zuora will automatically create a stored credential profile for the payment method, with the default value `Activate` set to this field.   # noqa: E501

        :return: The mit_profile_action of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._mit_profile_action

    @mit_profile_action.setter
    def mit_profile_action(self, mit_profile_action):
        """Sets the mit_profile_action of this POSTAccountTypePaymentMethod.

        Specifies how Zuora creates and activates the stored credential profile.  - `Activate` - Use this value if you are creating the stored credential profile after receiving the customer's consent.    Zuora will create the stored credential profile then send a cardholder-initiated transaction (CIT) to the payment gateway to validate the stored credential profile. If the CIT succeeds, the status of the stored credential profile will be `Active`. If the CIT does not succeed, Zuora will not create a stored credential profile.      If the payment gateway does not support the stored credential transaction framework, the status of the stored credential profile will be `Agreed`.   - `Persist` - Use this value if the stored credential profile represents a stored credential profile in an external system. The status of the payment method's stored credential profile will be `Active`.  If you do not specify this field, Zuora will automatically create a stored credential profile for the payment method, with the default value `Activate` set to this field.   # noqa: E501

        :param mit_profile_action: The mit_profile_action of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """
        allowed_values = ["Activate", "Persist"]  # noqa: E501
        if (self._configuration.client_side_validation and
                mit_profile_action not in allowed_values):
            raise ValueError(
                "Invalid value for `mit_profile_action` ({0}), must be one of {1}"  # noqa: E501
                .format(mit_profile_action, allowed_values)
            )

        self._mit_profile_action = mit_profile_action

    @property
    def mit_profile_agreed_on(self):
        """Gets the mit_profile_agreed_on of this POSTAccountTypePaymentMethod.  # noqa: E501

        The date on which the profile is agreed. The date format is `yyyy-mm-dd`.   # noqa: E501

        :return: The mit_profile_agreed_on of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: date
        """
        return self._mit_profile_agreed_on

    @mit_profile_agreed_on.setter
    def mit_profile_agreed_on(self, mit_profile_agreed_on):
        """Sets the mit_profile_agreed_on of this POSTAccountTypePaymentMethod.

        The date on which the profile is agreed. The date format is `yyyy-mm-dd`.   # noqa: E501

        :param mit_profile_agreed_on: The mit_profile_agreed_on of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: date
        """

        self._mit_profile_agreed_on = mit_profile_agreed_on

    @property
    def mit_profile_type(self):
        """Gets the mit_profile_type of this POSTAccountTypePaymentMethod.  # noqa: E501

        Required if you set the `mitProfileAction` field. Indicates the type of the stored credential profile to process recurring or unsecheduled transactions. If you do not specify the `mitProfileAction` field, Zuora will automatically create a stored credential profile for the payment method, with the default value `Recurring` set to this field.   # noqa: E501

        :return: The mit_profile_type of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._mit_profile_type

    @mit_profile_type.setter
    def mit_profile_type(self, mit_profile_type):
        """Sets the mit_profile_type of this POSTAccountTypePaymentMethod.

        Required if you set the `mitProfileAction` field. Indicates the type of the stored credential profile to process recurring or unsecheduled transactions. If you do not specify the `mitProfileAction` field, Zuora will automatically create a stored credential profile for the payment method, with the default value `Recurring` set to this field.   # noqa: E501

        :param mit_profile_type: The mit_profile_type of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """
        allowed_values = ["Recurring", "Unscheduled"]  # noqa: E501
        if (self._configuration.client_side_validation and
                mit_profile_type not in allowed_values):
            raise ValueError(
                "Invalid value for `mit_profile_type` ({0}), must be one of {1}"  # noqa: E501
                .format(mit_profile_type, allowed_values)
            )

        self._mit_profile_type = mit_profile_type

    @property
    def security_code(self):
        """Gets the security_code of this POSTAccountTypePaymentMethod.  # noqa: E501

        CVV or CVV2 security code of the credit card.  To ensure PCI compliance, this value is not stored and cannot be queried.   # noqa: E501

        :return: The security_code of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._security_code

    @security_code.setter
    def security_code(self, security_code):
        """Sets the security_code of this POSTAccountTypePaymentMethod.

        CVV or CVV2 security code of the credit card.  To ensure PCI compliance, this value is not stored and cannot be queried.   # noqa: E501

        :param security_code: The security_code of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._security_code = security_code

    @property
    def address_line1(self):
        """Gets the address_line1 of this POSTAccountTypePaymentMethod.  # noqa: E501

        First address line, 255 characters or less.   # noqa: E501

        :return: The address_line1 of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._address_line1

    @address_line1.setter
    def address_line1(self, address_line1):
        """Sets the address_line1 of this POSTAccountTypePaymentMethod.

        First address line, 255 characters or less.   # noqa: E501

        :param address_line1: The address_line1 of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._address_line1 = address_line1

    @property
    def address_line2(self):
        """Gets the address_line2 of this POSTAccountTypePaymentMethod.  # noqa: E501

        Second address line, 255 characters or less.   # noqa: E501

        :return: The address_line2 of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._address_line2

    @address_line2.setter
    def address_line2(self, address_line2):
        """Sets the address_line2 of this POSTAccountTypePaymentMethod.

        Second address line, 255 characters or less.   # noqa: E501

        :param address_line2: The address_line2 of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._address_line2 = address_line2

    @property
    def bank_aba_code(self):
        """Gets the bank_aba_code of this POSTAccountTypePaymentMethod.  # noqa: E501

        The nine-digit routing number or ABA number used by banks. This field is only required if the `type` field is set to `ACH`.   # noqa: E501

        :return: The bank_aba_code of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._bank_aba_code

    @bank_aba_code.setter
    def bank_aba_code(self, bank_aba_code):
        """Sets the bank_aba_code of this POSTAccountTypePaymentMethod.

        The nine-digit routing number or ABA number used by banks. This field is only required if the `type` field is set to `ACH`.   # noqa: E501

        :param bank_aba_code: The bank_aba_code of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._bank_aba_code = bank_aba_code

    @property
    def bank_account_name(self):
        """Gets the bank_account_name of this POSTAccountTypePaymentMethod.  # noqa: E501

        The name of the account holder, which can be either a person or a company. This field is only required if the `type` field is set to `ACH`.  For ACH payment methods on the BlueSnap integration, see [Overview of BlueSnap gateway integration](https://knowledgecenter.zuora.com/Zuora_Billing/Billing_and_Payments/M_Payment_Gateways/Supported_Payment_Gateways/BlueSnap_Gateway/Overview_of_BlueSnap_gateway_integration#Payer_Name_Extraction) for more information about how Zuora splits the string in this field into two parts and passes them to BlueSnap's `firstName` and `lastName` fields.   # noqa: E501

        :return: The bank_account_name of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._bank_account_name

    @bank_account_name.setter
    def bank_account_name(self, bank_account_name):
        """Sets the bank_account_name of this POSTAccountTypePaymentMethod.

        The name of the account holder, which can be either a person or a company. This field is only required if the `type` field is set to `ACH`.  For ACH payment methods on the BlueSnap integration, see [Overview of BlueSnap gateway integration](https://knowledgecenter.zuora.com/Zuora_Billing/Billing_and_Payments/M_Payment_Gateways/Supported_Payment_Gateways/BlueSnap_Gateway/Overview_of_BlueSnap_gateway_integration#Payer_Name_Extraction) for more information about how Zuora splits the string in this field into two parts and passes them to BlueSnap's `firstName` and `lastName` fields.   # noqa: E501

        :param bank_account_name: The bank_account_name of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """
        if (self._configuration.client_side_validation and
                bank_account_name is not None and len(bank_account_name) > 70):
            raise ValueError("Invalid value for `bank_account_name`, length must be less than or equal to `70`")  # noqa: E501

        self._bank_account_name = bank_account_name

    @property
    def bank_account_number(self):
        """Gets the bank_account_number of this POSTAccountTypePaymentMethod.  # noqa: E501

        The bank account number associated with the ACH payment. This field is only required if the `type` field is set to `ACH`.   # noqa: E501

        :return: The bank_account_number of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._bank_account_number

    @bank_account_number.setter
    def bank_account_number(self, bank_account_number):
        """Sets the bank_account_number of this POSTAccountTypePaymentMethod.

        The bank account number associated with the ACH payment. This field is only required if the `type` field is set to `ACH`.   # noqa: E501

        :param bank_account_number: The bank_account_number of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """
        if (self._configuration.client_side_validation and
                bank_account_number is not None and len(bank_account_number) > 30):
            raise ValueError("Invalid value for `bank_account_number`, length must be less than or equal to `30`")  # noqa: E501

        self._bank_account_number = bank_account_number

    @property
    def bank_account_type(self):
        """Gets the bank_account_type of this POSTAccountTypePaymentMethod.  # noqa: E501

        The type of bank account associated with the ACH payment. This field is only required if the `type` field is set to `ACH`.  When creating an ACH payment method on Adyen, this field is required by Zuora but it is not required by Adyen. To create the ACH payment method successfully, specify a real value for this field if you can. If it is not possible to get the real value for it, specify any of the allowed values as a dummy value, `Checking` preferably.   # noqa: E501

        :return: The bank_account_type of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._bank_account_type

    @bank_account_type.setter
    def bank_account_type(self, bank_account_type):
        """Sets the bank_account_type of this POSTAccountTypePaymentMethod.

        The type of bank account associated with the ACH payment. This field is only required if the `type` field is set to `ACH`.  When creating an ACH payment method on Adyen, this field is required by Zuora but it is not required by Adyen. To create the ACH payment method successfully, specify a real value for this field if you can. If it is not possible to get the real value for it, specify any of the allowed values as a dummy value, `Checking` preferably.   # noqa: E501

        :param bank_account_type: The bank_account_type of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """
        allowed_values = ["BusinessChecking", "BusinessSaving", "Checking", "Saving"]  # noqa: E501
        if (self._configuration.client_side_validation and
                bank_account_type not in allowed_values):
            raise ValueError(
                "Invalid value for `bank_account_type` ({0}), must be one of {1}"  # noqa: E501
                .format(bank_account_type, allowed_values)
            )

        self._bank_account_type = bank_account_type

    @property
    def bank_name(self):
        """Gets the bank_name of this POSTAccountTypePaymentMethod.  # noqa: E501

        The name of the bank where the ACH payment account is held. This field is only required if the `type` field is set to `ACH`.  When creating an ACH payment method on Adyen, this field is required by Zuora but it is not required by Adyen. To create the ACH payment method successfully, specify a real value for this field if you can. If it is not possible to get the real value for it, specify a dummy value.   # noqa: E501

        :return: The bank_name of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._bank_name

    @bank_name.setter
    def bank_name(self, bank_name):
        """Sets the bank_name of this POSTAccountTypePaymentMethod.

        The name of the bank where the ACH payment account is held. This field is only required if the `type` field is set to `ACH`.  When creating an ACH payment method on Adyen, this field is required by Zuora but it is not required by Adyen. To create the ACH payment method successfully, specify a real value for this field if you can. If it is not possible to get the real value for it, specify a dummy value.   # noqa: E501

        :param bank_name: The bank_name of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """
        if (self._configuration.client_side_validation and
                bank_name is not None and len(bank_name) > 70):
            raise ValueError("Invalid value for `bank_name`, length must be less than or equal to `70`")  # noqa: E501

        self._bank_name = bank_name

    @property
    def city(self):
        """Gets the city of this POSTAccountTypePaymentMethod.  # noqa: E501

        City, 40 characters or less.  It is recommended to provide the city and country information when creating a payment method. The information will be used to process payments. If the information is not provided during payment method creation, the city and country data will be missing during payment processing.        # noqa: E501

        :return: The city of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._city

    @city.setter
    def city(self, city):
        """Sets the city of this POSTAccountTypePaymentMethod.

        City, 40 characters or less.  It is recommended to provide the city and country information when creating a payment method. The information will be used to process payments. If the information is not provided during payment method creation, the city and country data will be missing during payment processing.        # noqa: E501

        :param city: The city of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._city = city

    @property
    def country(self):
        """Gets the country of this POSTAccountTypePaymentMethod.  # noqa: E501

        Country, must be a valid country name or abbreviation.  See [Country Names and Their ISO Standard 2- and 3-Digit Codes](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/D_Country%2C_State%2C_and_Province_Codes/A_Country_Names_and_Their_ISO_Codes) for the list of supported country names and abbreviations.  It is recommended to provide the city and country information when creating a payment method. The information will be used to process payments. If the information is not provided during payment method creation, the city and country data will be missing during payment processing.   # noqa: E501

        :return: The country of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._country

    @country.setter
    def country(self, country):
        """Sets the country of this POSTAccountTypePaymentMethod.

        Country, must be a valid country name or abbreviation.  See [Country Names and Their ISO Standard 2- and 3-Digit Codes](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/D_Country%2C_State%2C_and_Province_Codes/A_Country_Names_and_Their_ISO_Codes) for the list of supported country names and abbreviations.  It is recommended to provide the city and country information when creating a payment method. The information will be used to process payments. If the information is not provided during payment method creation, the city and country data will be missing during payment processing.   # noqa: E501

        :param country: The country of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._country = country

    @property
    def phone(self):
        """Gets the phone of this POSTAccountTypePaymentMethod.  # noqa: E501

        Phone number, 40 characters or less.   # noqa: E501

        :return: The phone of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._phone

    @phone.setter
    def phone(self, phone):
        """Sets the phone of this POSTAccountTypePaymentMethod.

        Phone number, 40 characters or less.   # noqa: E501

        :param phone: The phone of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._phone = phone

    @property
    def state(self):
        """Gets the state of this POSTAccountTypePaymentMethod.  # noqa: E501

        State, must be a valid state name or 2-character abbreviation.  See [United States Standard State Codes](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/D_Country%2C_State%2C_and_Province_Codes/B_State_Names_and_2-Digit_Codes) and [Canadian Standard Province Codes](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/D_Country%2C_State%2C_and_Province_Codes/C_Canadian_Province_Names_and_2-Digit_Codes) for the list of supported names and abbreviations.   # noqa: E501

        :return: The state of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._state

    @state.setter
    def state(self, state):
        """Sets the state of this POSTAccountTypePaymentMethod.

        State, must be a valid state name or 2-character abbreviation.  See [United States Standard State Codes](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/D_Country%2C_State%2C_and_Province_Codes/B_State_Names_and_2-Digit_Codes) and [Canadian Standard Province Codes](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/D_Country%2C_State%2C_and_Province_Codes/C_Canadian_Province_Names_and_2-Digit_Codes) for the list of supported names and abbreviations.   # noqa: E501

        :param state: The state of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._state = state

    @property
    def zip_code(self):
        """Gets the zip_code of this POSTAccountTypePaymentMethod.  # noqa: E501

        Zip code, 20 characters or less.   # noqa: E501

        :return: The zip_code of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._zip_code

    @zip_code.setter
    def zip_code(self, zip_code):
        """Sets the zip_code of this POSTAccountTypePaymentMethod.

        Zip code, 20 characters or less.   # noqa: E501

        :param zip_code: The zip_code of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._zip_code = zip_code

    @property
    def account_key(self):
        """Gets the account_key of this POSTAccountTypePaymentMethod.  # noqa: E501

        Internal ID of the customer account that will own the payment method.   # noqa: E501

        :return: The account_key of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._account_key

    @account_key.setter
    def account_key(self, account_key):
        """Sets the account_key of this POSTAccountTypePaymentMethod.

        Internal ID of the customer account that will own the payment method.   # noqa: E501

        :param account_key: The account_key of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._account_key = account_key

    @property
    def auth_gateway(self):
        """Gets the auth_gateway of this POSTAccountTypePaymentMethod.  # noqa: E501

        Internal ID of the payment gateway that Zuora will use to authorize the payments that are made with the payment method.  If you do not set this field, Zuora will use one of the following payment gateways instead:  * The default payment gateway of the customer account that owns the payment method, if the `accountKey` field is set. * The default payment gateway of your Zuora tenant, if the `accountKey` field is not set.   # noqa: E501

        :return: The auth_gateway of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._auth_gateway

    @auth_gateway.setter
    def auth_gateway(self, auth_gateway):
        """Sets the auth_gateway of this POSTAccountTypePaymentMethod.

        Internal ID of the payment gateway that Zuora will use to authorize the payments that are made with the payment method.  If you do not set this field, Zuora will use one of the following payment gateways instead:  * The default payment gateway of the customer account that owns the payment method, if the `accountKey` field is set. * The default payment gateway of your Zuora tenant, if the `accountKey` field is not set.   # noqa: E501

        :param auth_gateway: The auth_gateway of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._auth_gateway = auth_gateway

    @property
    def ip_address(self):
        """Gets the ip_address of this POSTAccountTypePaymentMethod.  # noqa: E501

        The IPv4 or IPv6 information of the user when the payment method is created or updated. Some gateways use this field for fraud prevention. If this field is passed to Zuora, Zuora directly passes it to gateways.   If the IP address length is beyond 45 characters, a validation error occurs.  For validating SEPA payment methods on Stripe v2, this field is required.   # noqa: E501

        :return: The ip_address of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._ip_address

    @ip_address.setter
    def ip_address(self, ip_address):
        """Sets the ip_address of this POSTAccountTypePaymentMethod.

        The IPv4 or IPv6 information of the user when the payment method is created or updated. Some gateways use this field for fraud prevention. If this field is passed to Zuora, Zuora directly passes it to gateways.   If the IP address length is beyond 45 characters, a validation error occurs.  For validating SEPA payment methods on Stripe v2, this field is required.   # noqa: E501

        :param ip_address: The ip_address of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._ip_address = ip_address

    @property
    def make_default(self):
        """Gets the make_default of this POSTAccountTypePaymentMethod.  # noqa: E501

        Specifies whether the payment method will be the default payment method of the customer account that owns the payment method. Only applicable if the `accountKey` field is set.   # noqa: E501

        :return: The make_default of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: bool
        """
        return self._make_default

    @make_default.setter
    def make_default(self, make_default):
        """Sets the make_default of this POSTAccountTypePaymentMethod.

        Specifies whether the payment method will be the default payment method of the customer account that owns the payment method. Only applicable if the `accountKey` field is set.   # noqa: E501

        :param make_default: The make_default of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: bool
        """

        self._make_default = make_default

    @property
    def iban(self):
        """Gets the iban of this POSTAccountTypePaymentMethod.  # noqa: E501

        The International Bank Account Number. This field is required if the `type` field is set to `SEPA`.   # noqa: E501

        :return: The iban of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._iban

    @iban.setter
    def iban(self, iban):
        """Sets the iban of this POSTAccountTypePaymentMethod.

        The International Bank Account Number. This field is required if the `type` field is set to `SEPA`.   # noqa: E501

        :param iban: The iban of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._iban = iban

    @property
    def account_holder_info(self):
        """Gets the account_holder_info of this POSTAccountTypePaymentMethod.  # noqa: E501


        :return: The account_holder_info of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: CreatePaymentMethodBankTransferAccountHolderInfo
        """
        return self._account_holder_info

    @account_holder_info.setter
    def account_holder_info(self, account_holder_info):
        """Sets the account_holder_info of this POSTAccountTypePaymentMethod.


        :param account_holder_info: The account_holder_info of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: CreatePaymentMethodBankTransferAccountHolderInfo
        """

        self._account_holder_info = account_holder_info

    @property
    def account_number(self):
        """Gets the account_number of this POSTAccountTypePaymentMethod.  # noqa: E501

        The number of the customer's bank account. This field is required for the following bank transfer payment methods:   - Direct Entry AU (`Becs`)   - Direct Debit NZ (`Becsnz`)   - Direct Debit UK (`Bacs`)   - Denmark Direct Debit (`Betalingsservice`)   - Sweden Direct Debit (`Autogiro`)   - Canadian Pre-Authorized Debit (`PAD`)   # noqa: E501

        :return: The account_number of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._account_number

    @account_number.setter
    def account_number(self, account_number):
        """Sets the account_number of this POSTAccountTypePaymentMethod.

        The number of the customer's bank account. This field is required for the following bank transfer payment methods:   - Direct Entry AU (`Becs`)   - Direct Debit NZ (`Becsnz`)   - Direct Debit UK (`Bacs`)   - Denmark Direct Debit (`Betalingsservice`)   - Sweden Direct Debit (`Autogiro`)   - Canadian Pre-Authorized Debit (`PAD`)   # noqa: E501

        :param account_number: The account_number of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._account_number = account_number

    @property
    def bank_code(self):
        """Gets the bank_code of this POSTAccountTypePaymentMethod.  # noqa: E501

        The sort code or number that identifies the bank. This is also known as the sort code. This field is required for the following bank transfer payment methods:   - Direct Debit UK (`Bacs`)   - Denmark Direct Debit (`Betalingsservice`)   - Direct Debit NZ (`Becsnz`)   - Canadian Pre-Authorized Debit (`PAD`)   # noqa: E501

        :return: The bank_code of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._bank_code

    @bank_code.setter
    def bank_code(self, bank_code):
        """Sets the bank_code of this POSTAccountTypePaymentMethod.

        The sort code or number that identifies the bank. This is also known as the sort code. This field is required for the following bank transfer payment methods:   - Direct Debit UK (`Bacs`)   - Denmark Direct Debit (`Betalingsservice`)   - Direct Debit NZ (`Becsnz`)   - Canadian Pre-Authorized Debit (`PAD`)   # noqa: E501

        :param bank_code: The bank_code of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._bank_code = bank_code

    @property
    def branch_code(self):
        """Gets the branch_code of this POSTAccountTypePaymentMethod.  # noqa: E501

        The branch code of the bank used for direct debit. This field is required for the following bank transfer payment methods:   - Sweden Direct Debit (`Autogiro`)   - Direct Entry AU (`Becs`)   - Direct Debit NZ (`Becsnz`)   - Canadian Pre-Authorized Debit (`PAD`)   # noqa: E501

        :return: The branch_code of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._branch_code

    @branch_code.setter
    def branch_code(self, branch_code):
        """Sets the branch_code of this POSTAccountTypePaymentMethod.

        The branch code of the bank used for direct debit. This field is required for the following bank transfer payment methods:   - Sweden Direct Debit (`Autogiro`)   - Direct Entry AU (`Becs`)   - Direct Debit NZ (`Becsnz`)   - Canadian Pre-Authorized Debit (`PAD`)   # noqa: E501

        :param branch_code: The branch_code of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._branch_code = branch_code

    @property
    def business_identification_code(self):
        """Gets the business_identification_code of this POSTAccountTypePaymentMethod.  # noqa: E501

        The BIC code used for SEPA.   # noqa: E501

        :return: The business_identification_code of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._business_identification_code

    @business_identification_code.setter
    def business_identification_code(self, business_identification_code):
        """Sets the business_identification_code of this POSTAccountTypePaymentMethod.

        The BIC code used for SEPA.   # noqa: E501

        :param business_identification_code: The business_identification_code of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._business_identification_code = business_identification_code

    @property
    def currency_code(self):
        """Gets the currency_code of this POSTAccountTypePaymentMethod.  # noqa: E501

        The currency used for payment method authorization.  If this field is not specified, `currency` specified for the account is used for payment method authorization. If no currency is specified for the account, the default currency of the account is then used.   # noqa: E501

        :return: The currency_code of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._currency_code

    @currency_code.setter
    def currency_code(self, currency_code):
        """Sets the currency_code of this POSTAccountTypePaymentMethod.

        The currency used for payment method authorization.  If this field is not specified, `currency` specified for the account is used for payment method authorization. If no currency is specified for the account, the default currency of the account is then used.   # noqa: E501

        :param currency_code: The currency_code of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._currency_code = currency_code

    @property
    def identity_number(self):
        """Gets the identity_number of this POSTAccountTypePaymentMethod.  # noqa: E501

        The identity number used for Bank Transfer. This field is required for the following bank transfer payment methods:   - Denmark Direct Debit (`Betalingsservice`)   - Sweden Direct Debit (`Autogiro`)   # noqa: E501

        :return: The identity_number of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._identity_number

    @identity_number.setter
    def identity_number(self, identity_number):
        """Sets the identity_number of this POSTAccountTypePaymentMethod.

        The identity number used for Bank Transfer. This field is required for the following bank transfer payment methods:   - Denmark Direct Debit (`Betalingsservice`)   - Sweden Direct Debit (`Autogiro`)   # noqa: E501

        :param identity_number: The identity_number of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._identity_number = identity_number

    @property
    def credit_card_mask_number(self):
        """Gets the credit_card_mask_number of this POSTAccountTypePaymentMethod.  # noqa: E501

        The masked credit card number, such as: ``` *********1112 ```  This field is specific for the CC Reference Transaction payment method. It is an optional field that you can use to distinguish different CC Reference Transaction payment methods.  Though there are no special restrictions on the input string, it is highly recommended to specify a card number that is masked.   # noqa: E501

        :return: The credit_card_mask_number of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._credit_card_mask_number

    @credit_card_mask_number.setter
    def credit_card_mask_number(self, credit_card_mask_number):
        """Sets the credit_card_mask_number of this POSTAccountTypePaymentMethod.

        The masked credit card number, such as: ``` *********1112 ```  This field is specific for the CC Reference Transaction payment method. It is an optional field that you can use to distinguish different CC Reference Transaction payment methods.  Though there are no special restrictions on the input string, it is highly recommended to specify a card number that is masked.   # noqa: E501

        :param credit_card_mask_number: The credit_card_mask_number of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """
        if (self._configuration.client_side_validation and
                credit_card_mask_number is not None and len(credit_card_mask_number) > 19):
            raise ValueError("Invalid value for `credit_card_mask_number`, length must be less than or equal to `19`")  # noqa: E501

        self._credit_card_mask_number = credit_card_mask_number

    @property
    def second_token_id(self):
        """Gets the second_token_id of this POSTAccountTypePaymentMethod.  # noqa: E501

        A gateway unique identifier that replaces sensitive payment method data.   `secondTokenId` is conditionally required only when `tokenId` is being used to represent a gateway customer profile. `secondTokenId` is used in the CC Reference Transaction payment method.   # noqa: E501

        :return: The second_token_id of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._second_token_id

    @second_token_id.setter
    def second_token_id(self, second_token_id):
        """Sets the second_token_id of this POSTAccountTypePaymentMethod.

        A gateway unique identifier that replaces sensitive payment method data.   `secondTokenId` is conditionally required only when `tokenId` is being used to represent a gateway customer profile. `secondTokenId` is used in the CC Reference Transaction payment method.   # noqa: E501

        :param second_token_id: The second_token_id of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._second_token_id = second_token_id

    @property
    def token_id(self):
        """Gets the token_id of this POSTAccountTypePaymentMethod.  # noqa: E501

        A gateway unique identifier that replaces sensitive payment method data or represents a gateway's unique customer profile. `tokenId` is required for the CC Reference Transaction payment method.  When `tokenId` is used to represent a customer profile, `secondTokenId` is conditionally required for representing the underlying tokenized payment method.  The values for the `tokenId` and `secondTokenId` fields differ for gateways. For more information, see the Knowledge Center article specific to each gateway that supports the CC Reference Transaction payment method.  **Note:** When creating an ACH payment method, if you need to pass in tokenized information, use the `mandateId` instead of `tokenId` field.   # noqa: E501

        :return: The token_id of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._token_id

    @token_id.setter
    def token_id(self, token_id):
        """Sets the token_id of this POSTAccountTypePaymentMethod.

        A gateway unique identifier that replaces sensitive payment method data or represents a gateway's unique customer profile. `tokenId` is required for the CC Reference Transaction payment method.  When `tokenId` is used to represent a customer profile, `secondTokenId` is conditionally required for representing the underlying tokenized payment method.  The values for the `tokenId` and `secondTokenId` fields differ for gateways. For more information, see the Knowledge Center article specific to each gateway that supports the CC Reference Transaction payment method.  **Note:** When creating an ACH payment method, if you need to pass in tokenized information, use the `mandateId` instead of `tokenId` field.   # noqa: E501

        :param token_id: The token_id of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._token_id = token_id

    @property
    def apple_payment_data(self):
        """Gets the apple_payment_data of this POSTAccountTypePaymentMethod.  # noqa: E501

        This field is specific for setting up Apple Pay for Adyen to include payload with Apple Pay token or Apple payment data. This information should be stringified. For more information, see [Set up Adyen Apple Pay](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/L_Payment_Methods/Payment_Method_Types/Apple_Pay_on_Web/Set_up_Adyen_Apple_Pay).   # noqa: E501

        :return: The apple_payment_data of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._apple_payment_data

    @apple_payment_data.setter
    def apple_payment_data(self, apple_payment_data):
        """Sets the apple_payment_data of this POSTAccountTypePaymentMethod.

        This field is specific for setting up Apple Pay for Adyen to include payload with Apple Pay token or Apple payment data. This information should be stringified. For more information, see [Set up Adyen Apple Pay](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/L_Payment_Methods/Payment_Method_Types/Apple_Pay_on_Web/Set_up_Adyen_Apple_Pay).   # noqa: E501

        :param apple_payment_data: The apple_payment_data of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._apple_payment_data = apple_payment_data

    @property
    def google_payment_token(self):
        """Gets the google_payment_token of this POSTAccountTypePaymentMethod.  # noqa: E501

        This field is specific for setting up Google Pay for Adyen and Chase gateway integrations to specify the stringified Google Pay token. For more information, see [Set up Adyen Google Pay](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/L_Payment_Methods/Payment_Method_Types/Set_up_Adyen_Google_Pay) and [Set up Google Pay on Chase](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/L_Payment_Methods/Payment_Method_Types/Set_up_Google_Pay_on_Chase).   # noqa: E501

        :return: The google_payment_token of this POSTAccountTypePaymentMethod.  # noqa: E501
        :rtype: str
        """
        return self._google_payment_token

    @google_payment_token.setter
    def google_payment_token(self, google_payment_token):
        """Sets the google_payment_token of this POSTAccountTypePaymentMethod.

        This field is specific for setting up Google Pay for Adyen and Chase gateway integrations to specify the stringified Google Pay token. For more information, see [Set up Adyen Google Pay](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/L_Payment_Methods/Payment_Method_Types/Set_up_Adyen_Google_Pay) and [Set up Google Pay on Chase](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/L_Payment_Methods/Payment_Method_Types/Set_up_Google_Pay_on_Chase).   # noqa: E501

        :param google_payment_token: The google_payment_token of this POSTAccountTypePaymentMethod.  # noqa: E501
        :type: str
        """

        self._google_payment_token = google_payment_token

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
        if issubclass(POSTAccountTypePaymentMethod, dict):
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
        if not isinstance(other, POSTAccountTypePaymentMethod):
            return False

        return self.to_dict() == other.to_dict()

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        if not isinstance(other, POSTAccountTypePaymentMethod):
            return True

        return self.to_dict() != other.to_dict()
