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


class POSTOfferChargeConfiguration(object):
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
        'bill_cycle_day': 'float',
        'bill_cycle_type': 'str',
        'billing_period': 'str',
        'billing_period_alignment': 'str',
        'billing_timing': 'str',
        'end_date_condition': 'str',
        'specific_billing_period': 'float',
        'trigger_event': 'str',
        'up_to_periods': 'float',
        'up_to_periods_type': 'str',
        'weekly_bill_cycle_day': 'str'
    }

    attribute_map = {
        'bill_cycle_day': 'billCycleDay',
        'bill_cycle_type': 'billCycleType',
        'billing_period': 'billingPeriod',
        'billing_period_alignment': 'billingPeriodAlignment',
        'billing_timing': 'billingTiming',
        'end_date_condition': 'endDateCondition',
        'specific_billing_period': 'specificBillingPeriod',
        'trigger_event': 'triggerEvent',
        'up_to_periods': 'upToPeriods',
        'up_to_periods_type': 'upToPeriodsType',
        'weekly_bill_cycle_day': 'weeklyBillCycleDay'
    }

    def __init__(self, bill_cycle_day=None, bill_cycle_type=None, billing_period=None, billing_period_alignment=None, billing_timing=None, end_date_condition='Subscription_End', specific_billing_period=None, trigger_event=None, up_to_periods=None, up_to_periods_type=None, weekly_bill_cycle_day=None, _configuration=None):  # noqa: E501
        """POSTOfferChargeConfiguration - a model defined in Swagger"""  # noqa: E501
        if _configuration is None:
            _configuration = Configuration()
        self._configuration = _configuration

        self._bill_cycle_day = None
        self._bill_cycle_type = None
        self._billing_period = None
        self._billing_period_alignment = None
        self._billing_timing = None
        self._end_date_condition = None
        self._specific_billing_period = None
        self._trigger_event = None
        self._up_to_periods = None
        self._up_to_periods_type = None
        self._weekly_bill_cycle_day = None
        self.discriminator = None

        if bill_cycle_day is not None:
            self.bill_cycle_day = bill_cycle_day
        if bill_cycle_type is not None:
            self.bill_cycle_type = bill_cycle_type
        if billing_period is not None:
            self.billing_period = billing_period
        if billing_period_alignment is not None:
            self.billing_period_alignment = billing_period_alignment
        if billing_timing is not None:
            self.billing_timing = billing_timing
        if end_date_condition is not None:
            self.end_date_condition = end_date_condition
        if specific_billing_period is not None:
            self.specific_billing_period = specific_billing_period
        if trigger_event is not None:
            self.trigger_event = trigger_event
        if up_to_periods is not None:
            self.up_to_periods = up_to_periods
        if up_to_periods_type is not None:
            self.up_to_periods_type = up_to_periods_type
        if weekly_bill_cycle_day is not None:
            self.weekly_bill_cycle_day = weekly_bill_cycle_day

    @property
    def bill_cycle_day(self):
        """Gets the bill_cycle_day of this POSTOfferChargeConfiguration.  # noqa: E501

        The day of the month when a bill run generates invoices for the account.  The bill cycle day (BCD) determines which day of the month customer is billed. The BCD value in the account can override the BCD in this object.   # noqa: E501

        :return: The bill_cycle_day of this POSTOfferChargeConfiguration.  # noqa: E501
        :rtype: float
        """
        return self._bill_cycle_day

    @bill_cycle_day.setter
    def bill_cycle_day(self, bill_cycle_day):
        """Sets the bill_cycle_day of this POSTOfferChargeConfiguration.

        The day of the month when a bill run generates invoices for the account.  The bill cycle day (BCD) determines which day of the month customer is billed. The BCD value in the account can override the BCD in this object.   # noqa: E501

        :param bill_cycle_day: The bill_cycle_day of this POSTOfferChargeConfiguration.  # noqa: E501
        :type: float
        """
        if (self._configuration.client_side_validation and
                bill_cycle_day is not None and bill_cycle_day > 31):  # noqa: E501
            raise ValueError("Invalid value for `bill_cycle_day`, must be a value less than or equal to `31`")  # noqa: E501
        if (self._configuration.client_side_validation and
                bill_cycle_day is not None and bill_cycle_day < 1):  # noqa: E501
            raise ValueError("Invalid value for `bill_cycle_day`, must be a value greater than or equal to `1`")  # noqa: E501

        self._bill_cycle_day = bill_cycle_day

    @property
    def bill_cycle_type(self):
        """Gets the bill_cycle_type of this POSTOfferChargeConfiguration.  # noqa: E501

        How to determine the billing day for the charge.   - If you set this field to `SpecificDayofMonth`, you must also specify the `billCycleDay` field.  - If you set this field to to `SpecificDayofWeek`, you must also specify the `weeklyBillCycleDay` field.   # noqa: E501

        :return: The bill_cycle_type of this POSTOfferChargeConfiguration.  # noqa: E501
        :rtype: str
        """
        return self._bill_cycle_type

    @bill_cycle_type.setter
    def bill_cycle_type(self, bill_cycle_type):
        """Sets the bill_cycle_type of this POSTOfferChargeConfiguration.

        How to determine the billing day for the charge.   - If you set this field to `SpecificDayofMonth`, you must also specify the `billCycleDay` field.  - If you set this field to to `SpecificDayofWeek`, you must also specify the `weeklyBillCycleDay` field.   # noqa: E501

        :param bill_cycle_type: The bill_cycle_type of this POSTOfferChargeConfiguration.  # noqa: E501
        :type: str
        """
        allowed_values = ["DefaultFromCustomer", "SpecificDayofMonth", "SubscriptionStartDay", "ChargeTriggerDay", "SpecificDayofWeek", "TermStartDay", "TermEndDay"]  # noqa: E501
        if (self._configuration.client_side_validation and
                bill_cycle_type not in allowed_values):
            raise ValueError(
                "Invalid value for `bill_cycle_type` ({0}), must be one of {1}"  # noqa: E501
                .format(bill_cycle_type, allowed_values)
            )

        self._bill_cycle_type = bill_cycle_type

    @property
    def billing_period(self):
        """Gets the billing_period of this POSTOfferChargeConfiguration.  # noqa: E501

        The  billing period for this product rate plan charge. The start day of the billing period is also called the bill cycle day (BCD).   # noqa: E501

        :return: The billing_period of this POSTOfferChargeConfiguration.  # noqa: E501
        :rtype: str
        """
        return self._billing_period

    @billing_period.setter
    def billing_period(self, billing_period):
        """Sets the billing_period of this POSTOfferChargeConfiguration.

        The  billing period for this product rate plan charge. The start day of the billing period is also called the bill cycle day (BCD).   # noqa: E501

        :param billing_period: The billing_period of this POSTOfferChargeConfiguration.  # noqa: E501
        :type: str
        """
        allowed_values = ["Month", "Quarter", "Semi_Annual", "Annual", "Eighteen_Months", "Two_Years", "Three_Years", "Five_Years", "Specific_Months", "Subscription_Term", "Week", "Specific_Weeks"]  # noqa: E501
        if (self._configuration.client_side_validation and
                billing_period not in allowed_values):
            raise ValueError(
                "Invalid value for `billing_period` ({0}), must be one of {1}"  # noqa: E501
                .format(billing_period, allowed_values)
            )

        self._billing_period = billing_period

    @property
    def billing_period_alignment(self):
        """Gets the billing_period_alignment of this POSTOfferChargeConfiguration.  # noqa: E501

        How to align charges within the same subscription if multiple charges begin on different dates.  **Note:** `AlignToTermEnd` is only available for prepayment charges by default. To enable this value for non-prepaid recurring charges, contact <a href=\"http://support.zuora.com/\" target=\"_blank\">Zuora Global Support</a>.   # noqa: E501

        :return: The billing_period_alignment of this POSTOfferChargeConfiguration.  # noqa: E501
        :rtype: str
        """
        return self._billing_period_alignment

    @billing_period_alignment.setter
    def billing_period_alignment(self, billing_period_alignment):
        """Sets the billing_period_alignment of this POSTOfferChargeConfiguration.

        How to align charges within the same subscription if multiple charges begin on different dates.  **Note:** `AlignToTermEnd` is only available for prepayment charges by default. To enable this value for non-prepaid recurring charges, contact <a href=\"http://support.zuora.com/\" target=\"_blank\">Zuora Global Support</a>.   # noqa: E501

        :param billing_period_alignment: The billing_period_alignment of this POSTOfferChargeConfiguration.  # noqa: E501
        :type: str
        """
        allowed_values = ["AlignToCharge", "AlignToSubscriptionStart", "AlignToTermStart", "AlignToTermEnd"]  # noqa: E501
        if (self._configuration.client_side_validation and
                billing_period_alignment not in allowed_values):
            raise ValueError(
                "Invalid value for `billing_period_alignment` ({0}), must be one of {1}"  # noqa: E501
                .format(billing_period_alignment, allowed_values)
            )

        self._billing_period_alignment = billing_period_alignment

    @property
    def billing_timing(self):
        """Gets the billing_timing of this POSTOfferChargeConfiguration.  # noqa: E501

        The billing timing for this product rate plan charge.  You can choose to bill in advance or in arrears for recurring charge types. This field is not used in one-time or usage based charge types.   # noqa: E501

        :return: The billing_timing of this POSTOfferChargeConfiguration.  # noqa: E501
        :rtype: str
        """
        return self._billing_timing

    @billing_timing.setter
    def billing_timing(self, billing_timing):
        """Sets the billing_timing of this POSTOfferChargeConfiguration.

        The billing timing for this product rate plan charge.  You can choose to bill in advance or in arrears for recurring charge types. This field is not used in one-time or usage based charge types.   # noqa: E501

        :param billing_timing: The billing_timing of this POSTOfferChargeConfiguration.  # noqa: E501
        :type: str
        """
        allowed_values = ["IN_ADVANCE", "IN_ARREARS"]  # noqa: E501
        if (self._configuration.client_side_validation and
                billing_timing not in allowed_values):
            raise ValueError(
                "Invalid value for `billing_timing` ({0}), must be one of {1}"  # noqa: E501
                .format(billing_timing, allowed_values)
            )

        self._billing_timing = billing_timing

    @property
    def end_date_condition(self):
        """Gets the end_date_condition of this POSTOfferChargeConfiguration.  # noqa: E501

        When the product rate plan charge ends after the charge trigger date.  - `Subscription_End`: The charge ends on the subscription end date after a specified period based on the trigger date of the charge. This is the default value.  - `Fixed_Period`: The charge ends after a specified period based on the trigger date of the charge. If you set this field to `Fixed_Period`, you must specify the length of the period and a period type by defining the `upToPeriods` and `upToPeriodsType` fields.   **Note**: If the subscription ends before the charge end date, the charge ends when the subscription ends. However, if the subscription end date is subsequently changed through a Renewal, or Terms and Conditions amendment, the charge will end on the charge end date.   # noqa: E501

        :return: The end_date_condition of this POSTOfferChargeConfiguration.  # noqa: E501
        :rtype: str
        """
        return self._end_date_condition

    @end_date_condition.setter
    def end_date_condition(self, end_date_condition):
        """Sets the end_date_condition of this POSTOfferChargeConfiguration.

        When the product rate plan charge ends after the charge trigger date.  - `Subscription_End`: The charge ends on the subscription end date after a specified period based on the trigger date of the charge. This is the default value.  - `Fixed_Period`: The charge ends after a specified period based on the trigger date of the charge. If you set this field to `Fixed_Period`, you must specify the length of the period and a period type by defining the `upToPeriods` and `upToPeriodsType` fields.   **Note**: If the subscription ends before the charge end date, the charge ends when the subscription ends. However, if the subscription end date is subsequently changed through a Renewal, or Terms and Conditions amendment, the charge will end on the charge end date.   # noqa: E501

        :param end_date_condition: The end_date_condition of this POSTOfferChargeConfiguration.  # noqa: E501
        :type: str
        """
        allowed_values = ["Subscription_End", "One_Time", "Fixed_Period", "Specific_End_Date"]  # noqa: E501
        if (self._configuration.client_side_validation and
                end_date_condition not in allowed_values):
            raise ValueError(
                "Invalid value for `end_date_condition` ({0}), must be one of {1}"  # noqa: E501
                .format(end_date_condition, allowed_values)
            )

        self._end_date_condition = end_date_condition

    @property
    def specific_billing_period(self):
        """Gets the specific_billing_period of this POSTOfferChargeConfiguration.  # noqa: E501

        The number of months or weeks for the billing period of this product rate plan charge.   This field is required if you set the `billingPeriod` field to `Specific_Months` or `Specific_Weeks`.   # noqa: E501

        :return: The specific_billing_period of this POSTOfferChargeConfiguration.  # noqa: E501
        :rtype: float
        """
        return self._specific_billing_period

    @specific_billing_period.setter
    def specific_billing_period(self, specific_billing_period):
        """Sets the specific_billing_period of this POSTOfferChargeConfiguration.

        The number of months or weeks for the billing period of this product rate plan charge.   This field is required if you set the `billingPeriod` field to `Specific_Months` or `Specific_Weeks`.   # noqa: E501

        :param specific_billing_period: The specific_billing_period of this POSTOfferChargeConfiguration.  # noqa: E501
        :type: float
        """

        self._specific_billing_period = specific_billing_period

    @property
    def trigger_event(self):
        """Gets the trigger_event of this POSTOfferChargeConfiguration.  # noqa: E501

        When to start billing the customer for the product rate plan charge.  - `ContractEffective` indicates the date when the subscription's contract goes into effect and the charge is ready to be billed.  - `ServiceActivation` indicates the date when the services or products for a subscription have been activated and the customers have access.  - `CustomerAcceptance` indicates the date when the customer accepts the services or products for a subscription.   # noqa: E501

        :return: The trigger_event of this POSTOfferChargeConfiguration.  # noqa: E501
        :rtype: str
        """
        return self._trigger_event

    @trigger_event.setter
    def trigger_event(self, trigger_event):
        """Sets the trigger_event of this POSTOfferChargeConfiguration.

        When to start billing the customer for the product rate plan charge.  - `ContractEffective` indicates the date when the subscription's contract goes into effect and the charge is ready to be billed.  - `ServiceActivation` indicates the date when the services or products for a subscription have been activated and the customers have access.  - `CustomerAcceptance` indicates the date when the customer accepts the services or products for a subscription.   # noqa: E501

        :param trigger_event: The trigger_event of this POSTOfferChargeConfiguration.  # noqa: E501
        :type: str
        """
        allowed_values = ["ContractEffective", "ServiceActivation", "CustomerAcceptance", "SpecificDate"]  # noqa: E501
        if (self._configuration.client_side_validation and
                trigger_event not in allowed_values):
            raise ValueError(
                "Invalid value for `trigger_event` ({0}), must be one of {1}"  # noqa: E501
                .format(trigger_event, allowed_values)
            )

        self._trigger_event = trigger_event

    @property
    def up_to_periods(self):
        """Gets the up_to_periods of this POSTOfferChargeConfiguration.  # noqa: E501

        The length of the period during which the charge is active. If this period ends before the subscription ends, the charge ends when this period ends.  **Note**: - You must use this field together with the `upToPeriodsType` field to specify the time period. This field is applicable only if you set the `EndDateCondition` field to `Fixed_Period`.   - If the subscription end date is subsequently changed through a Renewal, or Terms and Conditions amendment, the charge end date will change accordingly up to the original period end.   # noqa: E501

        :return: The up_to_periods of this POSTOfferChargeConfiguration.  # noqa: E501
        :rtype: float
        """
        return self._up_to_periods

    @up_to_periods.setter
    def up_to_periods(self, up_to_periods):
        """Sets the up_to_periods of this POSTOfferChargeConfiguration.

        The length of the period during which the charge is active. If this period ends before the subscription ends, the charge ends when this period ends.  **Note**: - You must use this field together with the `upToPeriodsType` field to specify the time period. This field is applicable only if you set the `EndDateCondition` field to `Fixed_Period`.   - If the subscription end date is subsequently changed through a Renewal, or Terms and Conditions amendment, the charge end date will change accordingly up to the original period end.   # noqa: E501

        :param up_to_periods: The up_to_periods of this POSTOfferChargeConfiguration.  # noqa: E501
        :type: float
        """

        self._up_to_periods = up_to_periods

    @property
    def up_to_periods_type(self):
        """Gets the up_to_periods_type of this POSTOfferChargeConfiguration.  # noqa: E501

        The period type used to define when the charge ends.  **Note**:  - You must use this field together with the `upToPeriods` field to specify the time period. - This field is applicable only if you set the `endDateCondition` field to `Fixed_Period`.            # noqa: E501

        :return: The up_to_periods_type of this POSTOfferChargeConfiguration.  # noqa: E501
        :rtype: str
        """
        return self._up_to_periods_type

    @up_to_periods_type.setter
    def up_to_periods_type(self, up_to_periods_type):
        """Sets the up_to_periods_type of this POSTOfferChargeConfiguration.

        The period type used to define when the charge ends.  **Note**:  - You must use this field together with the `upToPeriods` field to specify the time period. - This field is applicable only if you set the `endDateCondition` field to `Fixed_Period`.            # noqa: E501

        :param up_to_periods_type: The up_to_periods_type of this POSTOfferChargeConfiguration.  # noqa: E501
        :type: str
        """
        allowed_values = ["Billing_Periods", "Days", "Weeks", "Months", "Years"]  # noqa: E501
        if (self._configuration.client_side_validation and
                up_to_periods_type not in allowed_values):
            raise ValueError(
                "Invalid value for `up_to_periods_type` ({0}), must be one of {1}"  # noqa: E501
                .format(up_to_periods_type, allowed_values)
            )

        self._up_to_periods_type = up_to_periods_type

    @property
    def weekly_bill_cycle_day(self):
        """Gets the weekly_bill_cycle_day of this POSTOfferChargeConfiguration.  # noqa: E501

        The day of the week when a bill run generates invoices for the account.   # noqa: E501

        :return: The weekly_bill_cycle_day of this POSTOfferChargeConfiguration.  # noqa: E501
        :rtype: str
        """
        return self._weekly_bill_cycle_day

    @weekly_bill_cycle_day.setter
    def weekly_bill_cycle_day(self, weekly_bill_cycle_day):
        """Sets the weekly_bill_cycle_day of this POSTOfferChargeConfiguration.

        The day of the week when a bill run generates invoices for the account.   # noqa: E501

        :param weekly_bill_cycle_day: The weekly_bill_cycle_day of this POSTOfferChargeConfiguration.  # noqa: E501
        :type: str
        """
        allowed_values = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]  # noqa: E501
        if (self._configuration.client_side_validation and
                weekly_bill_cycle_day not in allowed_values):
            raise ValueError(
                "Invalid value for `weekly_bill_cycle_day` ({0}), must be one of {1}"  # noqa: E501
                .format(weekly_bill_cycle_day, allowed_values)
            )

        self._weekly_bill_cycle_day = weekly_bill_cycle_day

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
        if issubclass(POSTOfferChargeConfiguration, dict):
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
        if not isinstance(other, POSTOfferChargeConfiguration):
            return False

        return self.to_dict() == other.to_dict()

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        if not isinstance(other, POSTOfferChargeConfiguration):
            return True

        return self.to_dict() != other.to_dict()
