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


class PUTSubscriptionSuspendType(object):
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
        'application_order': 'list[str]',
        'apply_credit': 'bool',
        'apply_credit_balance': 'bool',
        'booking_date': 'date',
        'collect': 'bool',
        'contract_effective_date': 'date',
        'credit_memo_reason_code': 'str',
        'document_date': 'date',
        'extends_term': 'bool',
        'invoice': 'bool',
        'invoice_collect': 'bool',
        'invoice_target_date': 'date',
        'order_date': 'date',
        'resume': 'bool',
        'resume_periods': 'str',
        'resume_periods_type': 'str',
        'resume_policy': 'str',
        'resume_specific_date': 'date',
        'run_billing': 'bool',
        'suspend_periods': 'str',
        'suspend_periods_type': 'str',
        'suspend_policy': 'str',
        'suspend_specific_date': 'date',
        'target_date': 'date'
    }

    attribute_map = {
        'application_order': 'applicationOrder',
        'apply_credit': 'applyCredit',
        'apply_credit_balance': 'applyCreditBalance',
        'booking_date': 'bookingDate',
        'collect': 'collect',
        'contract_effective_date': 'contractEffectiveDate',
        'credit_memo_reason_code': 'creditMemoReasonCode',
        'document_date': 'documentDate',
        'extends_term': 'extendsTerm',
        'invoice': 'invoice',
        'invoice_collect': 'invoiceCollect',
        'invoice_target_date': 'invoiceTargetDate',
        'order_date': 'orderDate',
        'resume': 'resume',
        'resume_periods': 'resumePeriods',
        'resume_periods_type': 'resumePeriodsType',
        'resume_policy': 'resumePolicy',
        'resume_specific_date': 'resumeSpecificDate',
        'run_billing': 'runBilling',
        'suspend_periods': 'suspendPeriods',
        'suspend_periods_type': 'suspendPeriodsType',
        'suspend_policy': 'suspendPolicy',
        'suspend_specific_date': 'suspendSpecificDate',
        'target_date': 'targetDate'
    }

    def __init__(self, application_order=None, apply_credit=None, apply_credit_balance=None, booking_date=None, collect=False, contract_effective_date=None, credit_memo_reason_code=None, document_date=None, extends_term=None, invoice=None, invoice_collect=None, invoice_target_date=None, order_date=None, resume=None, resume_periods=None, resume_periods_type=None, resume_policy=None, resume_specific_date=None, run_billing=False, suspend_periods=None, suspend_periods_type=None, suspend_policy=None, suspend_specific_date=None, target_date=None, _configuration=None):  # noqa: E501
        """PUTSubscriptionSuspendType - a model defined in Swagger"""  # noqa: E501
        if _configuration is None:
            _configuration = Configuration()
        self._configuration = _configuration

        self._application_order = None
        self._apply_credit = None
        self._apply_credit_balance = None
        self._booking_date = None
        self._collect = None
        self._contract_effective_date = None
        self._credit_memo_reason_code = None
        self._document_date = None
        self._extends_term = None
        self._invoice = None
        self._invoice_collect = None
        self._invoice_target_date = None
        self._order_date = None
        self._resume = None
        self._resume_periods = None
        self._resume_periods_type = None
        self._resume_policy = None
        self._resume_specific_date = None
        self._run_billing = None
        self._suspend_periods = None
        self._suspend_periods_type = None
        self._suspend_policy = None
        self._suspend_specific_date = None
        self._target_date = None
        self.discriminator = None

        if application_order is not None:
            self.application_order = application_order
        if apply_credit is not None:
            self.apply_credit = apply_credit
        if apply_credit_balance is not None:
            self.apply_credit_balance = apply_credit_balance
        if booking_date is not None:
            self.booking_date = booking_date
        if collect is not None:
            self.collect = collect
        if contract_effective_date is not None:
            self.contract_effective_date = contract_effective_date
        if credit_memo_reason_code is not None:
            self.credit_memo_reason_code = credit_memo_reason_code
        if document_date is not None:
            self.document_date = document_date
        if extends_term is not None:
            self.extends_term = extends_term
        if invoice is not None:
            self.invoice = invoice
        if invoice_collect is not None:
            self.invoice_collect = invoice_collect
        if invoice_target_date is not None:
            self.invoice_target_date = invoice_target_date
        if order_date is not None:
            self.order_date = order_date
        if resume is not None:
            self.resume = resume
        if resume_periods is not None:
            self.resume_periods = resume_periods
        if resume_periods_type is not None:
            self.resume_periods_type = resume_periods_type
        if resume_policy is not None:
            self.resume_policy = resume_policy
        if resume_specific_date is not None:
            self.resume_specific_date = resume_specific_date
        if run_billing is not None:
            self.run_billing = run_billing
        if suspend_periods is not None:
            self.suspend_periods = suspend_periods
        if suspend_periods_type is not None:
            self.suspend_periods_type = suspend_periods_type
        self.suspend_policy = suspend_policy
        if suspend_specific_date is not None:
            self.suspend_specific_date = suspend_specific_date
        if target_date is not None:
            self.target_date = target_date

    @property
    def application_order(self):
        """Gets the application_order of this PUTSubscriptionSuspendType.  # noqa: E501

        The priority order to apply credit memos and/or unapplied payments to an invoice. Possible item values are: `CreditMemo`, `UnappliedPayment`.  **Note:**   - This field is valid only if the `applyCredit` field is set to `true`.   - If no value is specified for this field, the default priority order is used, [\"CreditMemo\", \"UnappliedPayment\"], to apply credit memos first and then apply unapplied payments.   - If only one item is specified, only the items of the spedified type are applied to invoices. For example, if the value is `[\"CreditMemo\"]`, only credit memos are used to apply to invoices.   # noqa: E501

        :return: The application_order of this PUTSubscriptionSuspendType.  # noqa: E501
        :rtype: list[str]
        """
        return self._application_order

    @application_order.setter
    def application_order(self, application_order):
        """Sets the application_order of this PUTSubscriptionSuspendType.

        The priority order to apply credit memos and/or unapplied payments to an invoice. Possible item values are: `CreditMemo`, `UnappliedPayment`.  **Note:**   - This field is valid only if the `applyCredit` field is set to `true`.   - If no value is specified for this field, the default priority order is used, [\"CreditMemo\", \"UnappliedPayment\"], to apply credit memos first and then apply unapplied payments.   - If only one item is specified, only the items of the spedified type are applied to invoices. For example, if the value is `[\"CreditMemo\"]`, only credit memos are used to apply to invoices.   # noqa: E501

        :param application_order: The application_order of this PUTSubscriptionSuspendType.  # noqa: E501
        :type: list[str]
        """

        self._application_order = application_order

    @property
    def apply_credit(self):
        """Gets the apply_credit of this PUTSubscriptionSuspendType.  # noqa: E501

        If the value is true, the credit memo or unapplied payment on the order account will be automatically applied to the invoices generated by this order. The credit memo generated by this order will not be automatically applied to any invoices.   **Note:** This field is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled. The Invoice Settlement feature is generally available as of Zuora Billing Release 296 (March 2021). This feature includes Unapplied Payments, Credit and Debit Memo, and Invoice Item Settlement. If you want to enable Invoice Settlement, see [Invoice Settlement Enablement and Checklist Guide](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement/Invoice_Settlement_Migration_Checklist_and_Guide) for more information.   # noqa: E501

        :return: The apply_credit of this PUTSubscriptionSuspendType.  # noqa: E501
        :rtype: bool
        """
        return self._apply_credit

    @apply_credit.setter
    def apply_credit(self, apply_credit):
        """Sets the apply_credit of this PUTSubscriptionSuspendType.

        If the value is true, the credit memo or unapplied payment on the order account will be automatically applied to the invoices generated by this order. The credit memo generated by this order will not be automatically applied to any invoices.   **Note:** This field is only available if you have [Invoice Settlement](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement) enabled. The Invoice Settlement feature is generally available as of Zuora Billing Release 296 (March 2021). This feature includes Unapplied Payments, Credit and Debit Memo, and Invoice Item Settlement. If you want to enable Invoice Settlement, see [Invoice Settlement Enablement and Checklist Guide](https://knowledgecenter.zuora.com/Billing/Billing_and_Payments/Invoice_Settlement/Invoice_Settlement_Migration_Checklist_and_Guide) for more information.   # noqa: E501

        :param apply_credit: The apply_credit of this PUTSubscriptionSuspendType.  # noqa: E501
        :type: bool
        """

        self._apply_credit = apply_credit

    @property
    def apply_credit_balance(self):
        """Gets the apply_credit_balance of this PUTSubscriptionSuspendType.  # noqa: E501

        Whether to automatically apply a credit balance to an invoice.  If the value is `true`, the credit balance is applied to the invoice. If the value is `false`, no action is taken.   To view the credit balance adjustment, retrieve the details of the invoice using the Get Invoices method.  Prerequisite: `invoice` must be `true`.   **Note:**    - If you are using the field `invoiceCollect` rather than the field `invoice`, the `invoiceCollect` value must be `true`.   - This field is deprecated if you have the Invoice Settlement feature enabled.   # noqa: E501

        :return: The apply_credit_balance of this PUTSubscriptionSuspendType.  # noqa: E501
        :rtype: bool
        """
        return self._apply_credit_balance

    @apply_credit_balance.setter
    def apply_credit_balance(self, apply_credit_balance):
        """Sets the apply_credit_balance of this PUTSubscriptionSuspendType.

        Whether to automatically apply a credit balance to an invoice.  If the value is `true`, the credit balance is applied to the invoice. If the value is `false`, no action is taken.   To view the credit balance adjustment, retrieve the details of the invoice using the Get Invoices method.  Prerequisite: `invoice` must be `true`.   **Note:**    - If you are using the field `invoiceCollect` rather than the field `invoice`, the `invoiceCollect` value must be `true`.   - This field is deprecated if you have the Invoice Settlement feature enabled.   # noqa: E501

        :param apply_credit_balance: The apply_credit_balance of this PUTSubscriptionSuspendType.  # noqa: E501
        :type: bool
        """

        self._apply_credit_balance = apply_credit_balance

    @property
    def booking_date(self):
        """Gets the booking_date of this PUTSubscriptionSuspendType.  # noqa: E501

        The booking date that you want to set for the amendment contract when you suspend the subscription. If `resume` is `true`, which means you also choose to resume the subscription at some point, then this field is also the booking date for the Resume amendment contract.  This field must be in the `yyyy-mm-dd` format. The default value of this field is the current date when you make the API call.    # noqa: E501

        :return: The booking_date of this PUTSubscriptionSuspendType.  # noqa: E501
        :rtype: date
        """
        return self._booking_date

    @booking_date.setter
    def booking_date(self, booking_date):
        """Sets the booking_date of this PUTSubscriptionSuspendType.

        The booking date that you want to set for the amendment contract when you suspend the subscription. If `resume` is `true`, which means you also choose to resume the subscription at some point, then this field is also the booking date for the Resume amendment contract.  This field must be in the `yyyy-mm-dd` format. The default value of this field is the current date when you make the API call.    # noqa: E501

        :param booking_date: The booking_date of this PUTSubscriptionSuspendType.  # noqa: E501
        :type: date
        """

        self._booking_date = booking_date

    @property
    def collect(self):
        """Gets the collect of this PUTSubscriptionSuspendType.  # noqa: E501

        Collects an automatic payment for a subscription. The collection generated in this operation is only for this subscription, not for the entire customer account.  If the value is `true`, the automatic payment is collected. If the value is `false`, no action is taken.  Prerequisite: The `invoice` or `runBilling` field must be `true`.   **Note**: This field is only available if you set the `zuora-version` request header to `196.0` or later.   # noqa: E501

        :return: The collect of this PUTSubscriptionSuspendType.  # noqa: E501
        :rtype: bool
        """
        return self._collect

    @collect.setter
    def collect(self, collect):
        """Sets the collect of this PUTSubscriptionSuspendType.

        Collects an automatic payment for a subscription. The collection generated in this operation is only for this subscription, not for the entire customer account.  If the value is `true`, the automatic payment is collected. If the value is `false`, no action is taken.  Prerequisite: The `invoice` or `runBilling` field must be `true`.   **Note**: This field is only available if you set the `zuora-version` request header to `196.0` or later.   # noqa: E501

        :param collect: The collect of this PUTSubscriptionSuspendType.  # noqa: E501
        :type: bool
        """

        self._collect = collect

    @property
    def contract_effective_date(self):
        """Gets the contract_effective_date of this PUTSubscriptionSuspendType.  # noqa: E501

        The date when the customer notifies you that they want to amend their subscription.   # noqa: E501

        :return: The contract_effective_date of this PUTSubscriptionSuspendType.  # noqa: E501
        :rtype: date
        """
        return self._contract_effective_date

    @contract_effective_date.setter
    def contract_effective_date(self, contract_effective_date):
        """Sets the contract_effective_date of this PUTSubscriptionSuspendType.

        The date when the customer notifies you that they want to amend their subscription.   # noqa: E501

        :param contract_effective_date: The contract_effective_date of this PUTSubscriptionSuspendType.  # noqa: E501
        :type: date
        """

        self._contract_effective_date = contract_effective_date

    @property
    def credit_memo_reason_code(self):
        """Gets the credit_memo_reason_code of this PUTSubscriptionSuspendType.  # noqa: E501

        A code identifying the reason for the credit memo transaction that is generated by the request. The value must be an existing reason code. If you do not pass the field or pass the field with empty value, Zuora uses the default reason code.  # noqa: E501

        :return: The credit_memo_reason_code of this PUTSubscriptionSuspendType.  # noqa: E501
        :rtype: str
        """
        return self._credit_memo_reason_code

    @credit_memo_reason_code.setter
    def credit_memo_reason_code(self, credit_memo_reason_code):
        """Sets the credit_memo_reason_code of this PUTSubscriptionSuspendType.

        A code identifying the reason for the credit memo transaction that is generated by the request. The value must be an existing reason code. If you do not pass the field or pass the field with empty value, Zuora uses the default reason code.  # noqa: E501

        :param credit_memo_reason_code: The credit_memo_reason_code of this PUTSubscriptionSuspendType.  # noqa: E501
        :type: str
        """

        self._credit_memo_reason_code = credit_memo_reason_code

    @property
    def document_date(self):
        """Gets the document_date of this PUTSubscriptionSuspendType.  # noqa: E501

        The date of the billing document, in `yyyy-mm-dd` format. It represents the invoice date for invoices, credit memo date for credit memos, and debit memo date for debit memos.  - If this field is specified, the specified date is used as the billing document date.  - If this field is not specified, the date specified in the `targetDate` is used as the billing document date.   # noqa: E501

        :return: The document_date of this PUTSubscriptionSuspendType.  # noqa: E501
        :rtype: date
        """
        return self._document_date

    @document_date.setter
    def document_date(self, document_date):
        """Sets the document_date of this PUTSubscriptionSuspendType.

        The date of the billing document, in `yyyy-mm-dd` format. It represents the invoice date for invoices, credit memo date for credit memos, and debit memo date for debit memos.  - If this field is specified, the specified date is used as the billing document date.  - If this field is not specified, the date specified in the `targetDate` is used as the billing document date.   # noqa: E501

        :param document_date: The document_date of this PUTSubscriptionSuspendType.  # noqa: E501
        :type: date
        """

        self._document_date = document_date

    @property
    def extends_term(self):
        """Gets the extends_term of this PUTSubscriptionSuspendType.  # noqa: E501

        Whether to extend the subscription term by the length of time the suspension is in effect. Values: `true`, `false`.   # noqa: E501

        :return: The extends_term of this PUTSubscriptionSuspendType.  # noqa: E501
        :rtype: bool
        """
        return self._extends_term

    @extends_term.setter
    def extends_term(self, extends_term):
        """Sets the extends_term of this PUTSubscriptionSuspendType.

        Whether to extend the subscription term by the length of time the suspension is in effect. Values: `true`, `false`.   # noqa: E501

        :param extends_term: The extends_term of this PUTSubscriptionSuspendType.  # noqa: E501
        :type: bool
        """

        self._extends_term = extends_term

    @property
    def invoice(self):
        """Gets the invoice of this PUTSubscriptionSuspendType.  # noqa: E501

        **Note:** This field has been replaced by the `runBilling` field. The `invoice` field is only available for backward compatibility.   Creates an invoice for a subscription. The invoice generated in this operation is only for this subscription, not for the entire customer account.   If the value is `true`, an invoice is created. If the value is `false`, no action is taken. The default value is `false`.    This field is in Zuora REST API version control. Supported minor versions are `196.0` and `207.0`. To use this field in the method, you must set the zuora-version parameter to the minor version number in the request header.   # noqa: E501

        :return: The invoice of this PUTSubscriptionSuspendType.  # noqa: E501
        :rtype: bool
        """
        return self._invoice

    @invoice.setter
    def invoice(self, invoice):
        """Sets the invoice of this PUTSubscriptionSuspendType.

        **Note:** This field has been replaced by the `runBilling` field. The `invoice` field is only available for backward compatibility.   Creates an invoice for a subscription. The invoice generated in this operation is only for this subscription, not for the entire customer account.   If the value is `true`, an invoice is created. If the value is `false`, no action is taken. The default value is `false`.    This field is in Zuora REST API version control. Supported minor versions are `196.0` and `207.0`. To use this field in the method, you must set the zuora-version parameter to the minor version number in the request header.   # noqa: E501

        :param invoice: The invoice of this PUTSubscriptionSuspendType.  # noqa: E501
        :type: bool
        """

        self._invoice = invoice

    @property
    def invoice_collect(self):
        """Gets the invoice_collect of this PUTSubscriptionSuspendType.  # noqa: E501

        **Note:** This field has been replaced by the `invoice` field and the `collect` field. `invoiceCollect` is available only for backward compatibility.   **Note**: This field is only available if you set the `zuora-version` request header to `186.0`, `187.0`, `188.0`, or `189.0`.   # noqa: E501

        :return: The invoice_collect of this PUTSubscriptionSuspendType.  # noqa: E501
        :rtype: bool
        """
        return self._invoice_collect

    @invoice_collect.setter
    def invoice_collect(self, invoice_collect):
        """Sets the invoice_collect of this PUTSubscriptionSuspendType.

        **Note:** This field has been replaced by the `invoice` field and the `collect` field. `invoiceCollect` is available only for backward compatibility.   **Note**: This field is only available if you set the `zuora-version` request header to `186.0`, `187.0`, `188.0`, or `189.0`.   # noqa: E501

        :param invoice_collect: The invoice_collect of this PUTSubscriptionSuspendType.  # noqa: E501
        :type: bool
        """

        self._invoice_collect = invoice_collect

    @property
    def invoice_target_date(self):
        """Gets the invoice_target_date of this PUTSubscriptionSuspendType.  # noqa: E501

        **Note:** This field has been replaced by the `targetDate` field. The `invoiceTargetDate` field is only available for backward compatibility.   Date through which to calculate charges if an invoice is generated, as yyyy-mm-dd. Default is current date.   This field is in Zuora REST API version control. Supported minor versions are `207.0` and earlier.   # noqa: E501

        :return: The invoice_target_date of this PUTSubscriptionSuspendType.  # noqa: E501
        :rtype: date
        """
        return self._invoice_target_date

    @invoice_target_date.setter
    def invoice_target_date(self, invoice_target_date):
        """Sets the invoice_target_date of this PUTSubscriptionSuspendType.

        **Note:** This field has been replaced by the `targetDate` field. The `invoiceTargetDate` field is only available for backward compatibility.   Date through which to calculate charges if an invoice is generated, as yyyy-mm-dd. Default is current date.   This field is in Zuora REST API version control. Supported minor versions are `207.0` and earlier.   # noqa: E501

        :param invoice_target_date: The invoice_target_date of this PUTSubscriptionSuspendType.  # noqa: E501
        :type: date
        """

        self._invoice_target_date = invoice_target_date

    @property
    def order_date(self):
        """Gets the order_date of this PUTSubscriptionSuspendType.  # noqa: E501

        The date when the order is signed. If no additinal contractEffectiveDate is provided, this order will use this order date as the contract effective date. This field must be in the `yyyy-mm-dd` format. This field is required for Orders customers only, not applicable to Orders Harmonization customers.   # noqa: E501

        :return: The order_date of this PUTSubscriptionSuspendType.  # noqa: E501
        :rtype: date
        """
        return self._order_date

    @order_date.setter
    def order_date(self, order_date):
        """Sets the order_date of this PUTSubscriptionSuspendType.

        The date when the order is signed. If no additinal contractEffectiveDate is provided, this order will use this order date as the contract effective date. This field must be in the `yyyy-mm-dd` format. This field is required for Orders customers only, not applicable to Orders Harmonization customers.   # noqa: E501

        :param order_date: The order_date of this PUTSubscriptionSuspendType.  # noqa: E501
        :type: date
        """

        self._order_date = order_date

    @property
    def resume(self):
        """Gets the resume of this PUTSubscriptionSuspendType.  # noqa: E501

        Whether to set when to resume a subscription when creating a suspend amendment. Values: `true`, `false`.   # noqa: E501

        :return: The resume of this PUTSubscriptionSuspendType.  # noqa: E501
        :rtype: bool
        """
        return self._resume

    @resume.setter
    def resume(self, resume):
        """Sets the resume of this PUTSubscriptionSuspendType.

        Whether to set when to resume a subscription when creating a suspend amendment. Values: `true`, `false`.   # noqa: E501

        :param resume: The resume of this PUTSubscriptionSuspendType.  # noqa: E501
        :type: bool
        """

        self._resume = resume

    @property
    def resume_periods(self):
        """Gets the resume_periods of this PUTSubscriptionSuspendType.  # noqa: E501

        The length of the period used to specify when the subscription is resumed. The subscription resumption takes effect after a specified period based on the suspend date or today's date. You must use this field together with the `resumePeriodsType` field to specify the period.  **Note:** This field is only applicable when the `suspendPolicy` field is set to `FixedPeriodsFromToday` or `FixedPeriodsFromSuspendDate`.   # noqa: E501

        :return: The resume_periods of this PUTSubscriptionSuspendType.  # noqa: E501
        :rtype: str
        """
        return self._resume_periods

    @resume_periods.setter
    def resume_periods(self, resume_periods):
        """Sets the resume_periods of this PUTSubscriptionSuspendType.

        The length of the period used to specify when the subscription is resumed. The subscription resumption takes effect after a specified period based on the suspend date or today's date. You must use this field together with the `resumePeriodsType` field to specify the period.  **Note:** This field is only applicable when the `suspendPolicy` field is set to `FixedPeriodsFromToday` or `FixedPeriodsFromSuspendDate`.   # noqa: E501

        :param resume_periods: The resume_periods of this PUTSubscriptionSuspendType.  # noqa: E501
        :type: str
        """

        self._resume_periods = resume_periods

    @property
    def resume_periods_type(self):
        """Gets the resume_periods_type of this PUTSubscriptionSuspendType.  # noqa: E501

        The period type used to define when the subscription resumption takes effect. The subscription resumption takes effect after a specified period based on the suspend date or today's date. You must use this field together with the resumePeriods field to specify the period.  Values: `Day`, `Week`, `Month`, `Year`  **Note:** This field is only applicable when the `suspendPolicy` field is set to `FixedPeriodsFromToday` or `FixedPeriodsFromSuspendDate`.   # noqa: E501

        :return: The resume_periods_type of this PUTSubscriptionSuspendType.  # noqa: E501
        :rtype: str
        """
        return self._resume_periods_type

    @resume_periods_type.setter
    def resume_periods_type(self, resume_periods_type):
        """Sets the resume_periods_type of this PUTSubscriptionSuspendType.

        The period type used to define when the subscription resumption takes effect. The subscription resumption takes effect after a specified period based on the suspend date or today's date. You must use this field together with the resumePeriods field to specify the period.  Values: `Day`, `Week`, `Month`, `Year`  **Note:** This field is only applicable when the `suspendPolicy` field is set to `FixedPeriodsFromToday` or `FixedPeriodsFromSuspendDate`.   # noqa: E501

        :param resume_periods_type: The resume_periods_type of this PUTSubscriptionSuspendType.  # noqa: E501
        :type: str
        """

        self._resume_periods_type = resume_periods_type

    @property
    def resume_policy(self):
        """Gets the resume_policy of this PUTSubscriptionSuspendType.  # noqa: E501

        Resume methods. Specify a way to resume a subscription. Values:  * `Today`: The subscription resumption takes effect on today's date.  * `FixedPeriodsFromSuspendDate`: The subscription resumption takes effect after a specified period based on the suspend date. You must specify the `resumePeriods` and `resumePeriodsType` fields to define the period.  * `SpecificDate`: The subscription resumption takes effect on a specific date. You must define the specific date in the `resumeSpecificDate` field.  * `FixedPeriodsFromToday`: The subscription resumption takes effect after a specified period based on the today's date. You must specify the `resumePeriods` and `resumePeriodsType` fields to define the period. * `suspendDate`: The subscription resumption takes effect on the date of suspension of the subscription.   # noqa: E501

        :return: The resume_policy of this PUTSubscriptionSuspendType.  # noqa: E501
        :rtype: str
        """
        return self._resume_policy

    @resume_policy.setter
    def resume_policy(self, resume_policy):
        """Sets the resume_policy of this PUTSubscriptionSuspendType.

        Resume methods. Specify a way to resume a subscription. Values:  * `Today`: The subscription resumption takes effect on today's date.  * `FixedPeriodsFromSuspendDate`: The subscription resumption takes effect after a specified period based on the suspend date. You must specify the `resumePeriods` and `resumePeriodsType` fields to define the period.  * `SpecificDate`: The subscription resumption takes effect on a specific date. You must define the specific date in the `resumeSpecificDate` field.  * `FixedPeriodsFromToday`: The subscription resumption takes effect after a specified period based on the today's date. You must specify the `resumePeriods` and `resumePeriodsType` fields to define the period. * `suspendDate`: The subscription resumption takes effect on the date of suspension of the subscription.   # noqa: E501

        :param resume_policy: The resume_policy of this PUTSubscriptionSuspendType.  # noqa: E501
        :type: str
        """

        self._resume_policy = resume_policy

    @property
    def resume_specific_date(self):
        """Gets the resume_specific_date of this PUTSubscriptionSuspendType.  # noqa: E501

        A specific date when the subscription resumption takes effect, in the format yyyy-mm-dd.  **Note:** This field is only applicable only when the `resumePolicy` field is set to `SpecificDate`.  The value should not be earlier than the subscription suspension date.   # noqa: E501

        :return: The resume_specific_date of this PUTSubscriptionSuspendType.  # noqa: E501
        :rtype: date
        """
        return self._resume_specific_date

    @resume_specific_date.setter
    def resume_specific_date(self, resume_specific_date):
        """Sets the resume_specific_date of this PUTSubscriptionSuspendType.

        A specific date when the subscription resumption takes effect, in the format yyyy-mm-dd.  **Note:** This field is only applicable only when the `resumePolicy` field is set to `SpecificDate`.  The value should not be earlier than the subscription suspension date.   # noqa: E501

        :param resume_specific_date: The resume_specific_date of this PUTSubscriptionSuspendType.  # noqa: E501
        :type: date
        """

        self._resume_specific_date = resume_specific_date

    @property
    def run_billing(self):
        """Gets the run_billing of this PUTSubscriptionSuspendType.  # noqa: E501

        Creates an invoice for a subscription. If you have the Invoice Settlement feature enabled, a credit memo might also be created based on the [invoice and credit memo generation rule](https://knowledgecenter.zuora.com/CB_Billing/Invoice_Settlement/Credit_and_Debit_Memos/Rules_for_Generating_Invoices_and_Credit_Memos).     The billing documents generated in this operation is only for this subscription, not for the entire customer account.   Possible values:  - `true`: An invoice is created. If you have the Invoice Settlement feature enabled, a credit memo might also be created.   - `false`: No invoice is created.   **Note:** This field is in Zuora REST API version control. Supported minor versions are `211.0` or later. To use this field in the method, you must set the `zuora-version` parameter to the minor version number in the request header.   # noqa: E501

        :return: The run_billing of this PUTSubscriptionSuspendType.  # noqa: E501
        :rtype: bool
        """
        return self._run_billing

    @run_billing.setter
    def run_billing(self, run_billing):
        """Sets the run_billing of this PUTSubscriptionSuspendType.

        Creates an invoice for a subscription. If you have the Invoice Settlement feature enabled, a credit memo might also be created based on the [invoice and credit memo generation rule](https://knowledgecenter.zuora.com/CB_Billing/Invoice_Settlement/Credit_and_Debit_Memos/Rules_for_Generating_Invoices_and_Credit_Memos).     The billing documents generated in this operation is only for this subscription, not for the entire customer account.   Possible values:  - `true`: An invoice is created. If you have the Invoice Settlement feature enabled, a credit memo might also be created.   - `false`: No invoice is created.   **Note:** This field is in Zuora REST API version control. Supported minor versions are `211.0` or later. To use this field in the method, you must set the `zuora-version` parameter to the minor version number in the request header.   # noqa: E501

        :param run_billing: The run_billing of this PUTSubscriptionSuspendType.  # noqa: E501
        :type: bool
        """

        self._run_billing = run_billing

    @property
    def suspend_periods(self):
        """Gets the suspend_periods of this PUTSubscriptionSuspendType.  # noqa: E501

        The length of the period used to specify when the subscription suspension takes effect. The subscription suspension takes effect after a specified period based on today's date. You must use this field together with the `suspendPeriodsType` field to specify the period.  **Note:** This field is only applicable only when the suspendPolicy field is set to FixedPeriodsFromToday.   # noqa: E501

        :return: The suspend_periods of this PUTSubscriptionSuspendType.  # noqa: E501
        :rtype: str
        """
        return self._suspend_periods

    @suspend_periods.setter
    def suspend_periods(self, suspend_periods):
        """Sets the suspend_periods of this PUTSubscriptionSuspendType.

        The length of the period used to specify when the subscription suspension takes effect. The subscription suspension takes effect after a specified period based on today's date. You must use this field together with the `suspendPeriodsType` field to specify the period.  **Note:** This field is only applicable only when the suspendPolicy field is set to FixedPeriodsFromToday.   # noqa: E501

        :param suspend_periods: The suspend_periods of this PUTSubscriptionSuspendType.  # noqa: E501
        :type: str
        """

        self._suspend_periods = suspend_periods

    @property
    def suspend_periods_type(self):
        """Gets the suspend_periods_type of this PUTSubscriptionSuspendType.  # noqa: E501

        The period type used to define when the subscription suspension takes effect. The subscription suspension takes effect after a specified period based on today's date. You must use this field together with the suspendPeriods field to specify the period.  Type: string (enum)  Values: `Day`, `Week`, `Month`, `Year`  **Note:** This field is only applicable only when the suspendPolicy field is set to FixedPeriodsFromToday.   # noqa: E501

        :return: The suspend_periods_type of this PUTSubscriptionSuspendType.  # noqa: E501
        :rtype: str
        """
        return self._suspend_periods_type

    @suspend_periods_type.setter
    def suspend_periods_type(self, suspend_periods_type):
        """Sets the suspend_periods_type of this PUTSubscriptionSuspendType.

        The period type used to define when the subscription suspension takes effect. The subscription suspension takes effect after a specified period based on today's date. You must use this field together with the suspendPeriods field to specify the period.  Type: string (enum)  Values: `Day`, `Week`, `Month`, `Year`  **Note:** This field is only applicable only when the suspendPolicy field is set to FixedPeriodsFromToday.   # noqa: E501

        :param suspend_periods_type: The suspend_periods_type of this PUTSubscriptionSuspendType.  # noqa: E501
        :type: str
        """

        self._suspend_periods_type = suspend_periods_type

    @property
    def suspend_policy(self):
        """Gets the suspend_policy of this PUTSubscriptionSuspendType.  # noqa: E501

        Suspend methods. Specify a way to suspend a subscription.   Value:  * `Today`: The subscription suspension takes effect on today's date. * `EndOfLastInvoicePeriod`: The subscription suspension takes effect at the end of the last invoice period. The suspend date defaults to a date that is one day after the last invoiced period. You can choose this option to avoid any negative invoices (credits) issued back to the customer after the subscription suspension.  * `SpecificDate`: The subscription suspension takes effect on a specific date. You must define the specific date in the `suspendSpecificDate` field. * `FixedPeriodsFromToday`: The subscription suspension takes effect after a specified period based on today's date. You must specify the `suspendPeriods` and `suspendPeriodsType` fields to define the period.   # noqa: E501

        :return: The suspend_policy of this PUTSubscriptionSuspendType.  # noqa: E501
        :rtype: str
        """
        return self._suspend_policy

    @suspend_policy.setter
    def suspend_policy(self, suspend_policy):
        """Sets the suspend_policy of this PUTSubscriptionSuspendType.

        Suspend methods. Specify a way to suspend a subscription.   Value:  * `Today`: The subscription suspension takes effect on today's date. * `EndOfLastInvoicePeriod`: The subscription suspension takes effect at the end of the last invoice period. The suspend date defaults to a date that is one day after the last invoiced period. You can choose this option to avoid any negative invoices (credits) issued back to the customer after the subscription suspension.  * `SpecificDate`: The subscription suspension takes effect on a specific date. You must define the specific date in the `suspendSpecificDate` field. * `FixedPeriodsFromToday`: The subscription suspension takes effect after a specified period based on today's date. You must specify the `suspendPeriods` and `suspendPeriodsType` fields to define the period.   # noqa: E501

        :param suspend_policy: The suspend_policy of this PUTSubscriptionSuspendType.  # noqa: E501
        :type: str
        """
        if self._configuration.client_side_validation and suspend_policy is None:
            raise ValueError("Invalid value for `suspend_policy`, must not be `None`")  # noqa: E501

        self._suspend_policy = suspend_policy

    @property
    def suspend_specific_date(self):
        """Gets the suspend_specific_date of this PUTSubscriptionSuspendType.  # noqa: E501

        A specific date when the subscription suspension takes effect, in the format yyyy-mm-dd.  **Note:** This field is only applicable only when the suspendPolicy field is set to SpecificDate.  The value should not be earlier than the subscription contract effective date, later than the subscription term end date, or within a period for which the customer has been invoiced.   # noqa: E501

        :return: The suspend_specific_date of this PUTSubscriptionSuspendType.  # noqa: E501
        :rtype: date
        """
        return self._suspend_specific_date

    @suspend_specific_date.setter
    def suspend_specific_date(self, suspend_specific_date):
        """Sets the suspend_specific_date of this PUTSubscriptionSuspendType.

        A specific date when the subscription suspension takes effect, in the format yyyy-mm-dd.  **Note:** This field is only applicable only when the suspendPolicy field is set to SpecificDate.  The value should not be earlier than the subscription contract effective date, later than the subscription term end date, or within a period for which the customer has been invoiced.   # noqa: E501

        :param suspend_specific_date: The suspend_specific_date of this PUTSubscriptionSuspendType.  # noqa: E501
        :type: date
        """

        self._suspend_specific_date = suspend_specific_date

    @property
    def target_date(self):
        """Gets the target_date of this PUTSubscriptionSuspendType.  # noqa: E501

        Date through which to calculate charges if an invoice or a credit memo is generated, as yyyy-mm-dd. Default is current date.   **Note:** The credit memo is only available if you have the Invoice Settlement feature enabled.   This field is in Zuora REST API version control. Supported minor versions are `211.0` and later. To use this field in the method, you must set the  `zuora-version` parameter to the minor version number in the request header.   # noqa: E501

        :return: The target_date of this PUTSubscriptionSuspendType.  # noqa: E501
        :rtype: date
        """
        return self._target_date

    @target_date.setter
    def target_date(self, target_date):
        """Sets the target_date of this PUTSubscriptionSuspendType.

        Date through which to calculate charges if an invoice or a credit memo is generated, as yyyy-mm-dd. Default is current date.   **Note:** The credit memo is only available if you have the Invoice Settlement feature enabled.   This field is in Zuora REST API version control. Supported minor versions are `211.0` and later. To use this field in the method, you must set the  `zuora-version` parameter to the minor version number in the request header.   # noqa: E501

        :param target_date: The target_date of this PUTSubscriptionSuspendType.  # noqa: E501
        :type: date
        """

        self._target_date = target_date

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
        if issubclass(PUTSubscriptionSuspendType, dict):
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
        if not isinstance(other, PUTSubscriptionSuspendType):
            return False

        return self.to_dict() == other.to_dict()

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        if not isinstance(other, PUTSubscriptionSuspendType):
            return True

        return self.to_dict() != other.to_dict()
