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


class SubmitBatchQueryResponse(object):
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
        'batches': 'list[BatchesQueries]',
        'encrypted': 'str',
        'format': 'str',
        'id': 'str',
        'incremental_time': 'str',
        'name': 'str',
        'notify_url': 'str',
        'offset': 'float',
        'partner': 'str',
        'project': 'str',
        'status': 'str',
        'use_last_completed_job_queries': 'bool',
        'version': 'float'
    }

    attribute_map = {
        'batches': 'batches',
        'encrypted': 'encrypted',
        'format': 'format',
        'id': 'id',
        'incremental_time': 'incrementalTime',
        'name': 'name',
        'notify_url': 'notifyUrl',
        'offset': 'offset',
        'partner': 'partner',
        'project': 'project',
        'status': 'status',
        'use_last_completed_job_queries': 'useLastCompletedJobQueries',
        'version': 'version'
    }

    def __init__(self, batches=None, encrypted=None, format=None, id=None, incremental_time=None, name=None, notify_url=None, offset=None, partner=None, project=None, status=None, use_last_completed_job_queries=None, version=None, _configuration=None):  # noqa: E501
        """SubmitBatchQueryResponse - a model defined in Swagger"""  # noqa: E501
        if _configuration is None:
            _configuration = Configuration()
        self._configuration = _configuration

        self._batches = None
        self._encrypted = None
        self._format = None
        self._id = None
        self._incremental_time = None
        self._name = None
        self._notify_url = None
        self._offset = None
        self._partner = None
        self._project = None
        self._status = None
        self._use_last_completed_job_queries = None
        self._version = None
        self.discriminator = None

        if batches is not None:
            self.batches = batches
        if encrypted is not None:
            self.encrypted = encrypted
        if format is not None:
            self.format = format
        if id is not None:
            self.id = id
        if incremental_time is not None:
            self.incremental_time = incremental_time
        if name is not None:
            self.name = name
        if notify_url is not None:
            self.notify_url = notify_url
        if offset is not None:
            self.offset = offset
        if partner is not None:
            self.partner = partner
        if project is not None:
            self.project = project
        if status is not None:
            self.status = status
        if use_last_completed_job_queries is not None:
            self.use_last_completed_job_queries = use_last_completed_job_queries
        if version is not None:
            self.version = version

    @property
    def batches(self):
        """Gets the batches of this SubmitBatchQueryResponse.  # noqa: E501

        A JSON array object that contains a list of batch objects.   # noqa: E501

        :return: The batches of this SubmitBatchQueryResponse.  # noqa: E501
        :rtype: list[BatchesQueries]
        """
        return self._batches

    @batches.setter
    def batches(self, batches):
        """Sets the batches of this SubmitBatchQueryResponse.

        A JSON array object that contains a list of batch objects.   # noqa: E501

        :param batches: The batches of this SubmitBatchQueryResponse.  # noqa: E501
        :type: list[BatchesQueries]
        """

        self._batches = batches

    @property
    def encrypted(self):
        """Gets the encrypted of this SubmitBatchQueryResponse.  # noqa: E501

        If enabled, you must supply the formatting (zip or unzip) first and decrypt it to get the actual contents.         # noqa: E501

        :return: The encrypted of this SubmitBatchQueryResponse.  # noqa: E501
        :rtype: str
        """
        return self._encrypted

    @encrypted.setter
    def encrypted(self, encrypted):
        """Sets the encrypted of this SubmitBatchQueryResponse.

        If enabled, you must supply the formatting (zip or unzip) first and decrypt it to get the actual contents.         # noqa: E501

        :param encrypted: The encrypted of this SubmitBatchQueryResponse.  # noqa: E501
        :type: str
        """
        allowed_values = ["pgp", "none"]  # noqa: E501
        if (self._configuration.client_side_validation and
                encrypted not in allowed_values):
            raise ValueError(
                "Invalid value for `encrypted` ({0}), must be one of {1}"  # noqa: E501
                .format(encrypted, allowed_values)
            )

        self._encrypted = encrypted

    @property
    def format(self):
        """Gets the format of this SubmitBatchQueryResponse.  # noqa: E501

        The format of the query. The default value is `csv`.   # noqa: E501

        :return: The format of this SubmitBatchQueryResponse.  # noqa: E501
        :rtype: str
        """
        return self._format

    @format.setter
    def format(self, format):
        """Sets the format of this SubmitBatchQueryResponse.

        The format of the query. The default value is `csv`.   # noqa: E501

        :param format: The format of this SubmitBatchQueryResponse.  # noqa: E501
        :type: str
        """
        allowed_values = ["csv", "zip", "gzip"]  # noqa: E501
        if (self._configuration.client_side_validation and
                format not in allowed_values):
            raise ValueError(
                "Invalid value for `format` ({0}), must be one of {1}"  # noqa: E501
                .format(format, allowed_values)
            )

        self._format = format

    @property
    def id(self):
        """Gets the id of this SubmitBatchQueryResponse.  # noqa: E501

        The job ID created for the AQuA API request. The job ID can be used for querying for the query status.   The ID exists only if the JSON request can be parsed and validated successfully. Otherwise, the job ID is null.   # noqa: E501

        :return: The id of this SubmitBatchQueryResponse.  # noqa: E501
        :rtype: str
        """
        return self._id

    @id.setter
    def id(self, id):
        """Sets the id of this SubmitBatchQueryResponse.

        The job ID created for the AQuA API request. The job ID can be used for querying for the query status.   The ID exists only if the JSON request can be parsed and validated successfully. Otherwise, the job ID is null.   # noqa: E501

        :param id: The id of this SubmitBatchQueryResponse.  # noqa: E501
        :type: str
        """

        self._id = id

    @property
    def incremental_time(self):
        """Gets the incremental_time of this SubmitBatchQueryResponse.  # noqa: E501

        Allows you to override the time from which a Stateful AQuA job incrementally retrieves records that have been created or modified, using the `incrementalTime` parameter. For example, if you set `incrementalTime` = `2015-01-21 10:30:01`, AQuA will retrieve records that have created or modified beginning at 10:30:01. If this parameter is not set, AQuA continues to use the Start Time of the last AQuA session to retrieve records incrementally.  The time zone of `incrementalTime` depends on which Zuora data center you use. For US Data Center customers, the time zone of `incrementalTime` is Pacific Time. For EU Data Center customers, the time zone of `incrementalTime` is UTC. If the time zone of your system is different from the time zone of `incrementalTime`, you will need to convert to the appropriate time zone before setting `incrementalTime`.  **Note**: This field can only be used in Stateful AQuA mode.       # noqa: E501

        :return: The incremental_time of this SubmitBatchQueryResponse.  # noqa: E501
        :rtype: str
        """
        return self._incremental_time

    @incremental_time.setter
    def incremental_time(self, incremental_time):
        """Sets the incremental_time of this SubmitBatchQueryResponse.

        Allows you to override the time from which a Stateful AQuA job incrementally retrieves records that have been created or modified, using the `incrementalTime` parameter. For example, if you set `incrementalTime` = `2015-01-21 10:30:01`, AQuA will retrieve records that have created or modified beginning at 10:30:01. If this parameter is not set, AQuA continues to use the Start Time of the last AQuA session to retrieve records incrementally.  The time zone of `incrementalTime` depends on which Zuora data center you use. For US Data Center customers, the time zone of `incrementalTime` is Pacific Time. For EU Data Center customers, the time zone of `incrementalTime` is UTC. If the time zone of your system is different from the time zone of `incrementalTime`, you will need to convert to the appropriate time zone before setting `incrementalTime`.  **Note**: This field can only be used in Stateful AQuA mode.       # noqa: E501

        :param incremental_time: The incremental_time of this SubmitBatchQueryResponse.  # noqa: E501
        :type: str
        """

        self._incremental_time = incremental_time

    @property
    def name(self):
        """Gets the name of this SubmitBatchQueryResponse.  # noqa: E501

        The name of the job. 32 character limit.   # noqa: E501

        :return: The name of this SubmitBatchQueryResponse.  # noqa: E501
        :rtype: str
        """
        return self._name

    @name.setter
    def name(self, name):
        """Sets the name of this SubmitBatchQueryResponse.

        The name of the job. 32 character limit.   # noqa: E501

        :param name: The name of this SubmitBatchQueryResponse.  # noqa: E501
        :type: str
        """

        self._name = name

    @property
    def notify_url(self):
        """Gets the notify_url of this SubmitBatchQueryResponse.  # noqa: E501

        If URL is provided, the AQuA job will call this `notifyUrl` once the job has completed. The value of `notifyUrl` needs to have `${JOBID}` and `${STATUS}` placeholders. These placeholders will be replaced by the actual job ID and status when returned in the response. Status will be `Completed` after the AQuA job is done.  If you submit an AQuA query with `notifyUrl` specified, the value of `notifyUrl` will be ignored if your organization has already <a href=\"https://knowledgecenter.zuora.com/Zuora_Central_Platform/API/AB_Aggregate_Query_API/Callout_Notification_for_Completed_AQuA_Jobs\" target=\"_blank\">configured a callout notification through the Zuora user interface</a>.              # noqa: E501

        :return: The notify_url of this SubmitBatchQueryResponse.  # noqa: E501
        :rtype: str
        """
        return self._notify_url

    @notify_url.setter
    def notify_url(self, notify_url):
        """Sets the notify_url of this SubmitBatchQueryResponse.

        If URL is provided, the AQuA job will call this `notifyUrl` once the job has completed. The value of `notifyUrl` needs to have `${JOBID}` and `${STATUS}` placeholders. These placeholders will be replaced by the actual job ID and status when returned in the response. Status will be `Completed` after the AQuA job is done.  If you submit an AQuA query with `notifyUrl` specified, the value of `notifyUrl` will be ignored if your organization has already <a href=\"https://knowledgecenter.zuora.com/Zuora_Central_Platform/API/AB_Aggregate_Query_API/Callout_Notification_for_Completed_AQuA_Jobs\" target=\"_blank\">configured a callout notification through the Zuora user interface</a>.              # noqa: E501

        :param notify_url: The notify_url of this SubmitBatchQueryResponse.  # noqa: E501
        :type: str
        """

        self._notify_url = notify_url

    @property
    def offset(self):
        """Gets the offset of this SubmitBatchQueryResponse.  # noqa: E501

        This field specifies the time offset for AQuA queries in stateful mode. It is an integer in the range 0 to 3,600 seconds.  For example, if you set this field to 600 seconds and you post a query in stateful mode at 2:00 AM, it will query against data created or updated between the completion time of the previous query and 1:50 AM.  The value of this field will override the value you configured in **Settings** > **Administration** > **AQuA API Stateful Mode Time Offset**.       # noqa: E501

        :return: The offset of this SubmitBatchQueryResponse.  # noqa: E501
        :rtype: float
        """
        return self._offset

    @offset.setter
    def offset(self, offset):
        """Sets the offset of this SubmitBatchQueryResponse.

        This field specifies the time offset for AQuA queries in stateful mode. It is an integer in the range 0 to 3,600 seconds.  For example, if you set this field to 600 seconds and you post a query in stateful mode at 2:00 AM, it will query against data created or updated between the completion time of the previous query and 1:50 AM.  The value of this field will override the value you configured in **Settings** > **Administration** > **AQuA API Stateful Mode Time Offset**.       # noqa: E501

        :param offset: The offset of this SubmitBatchQueryResponse.  # noqa: E501
        :type: float
        """

        self._offset = offset

    @property
    def partner(self):
        """Gets the partner of this SubmitBatchQueryResponse.  # noqa: E501

        The partner field indicates the unique ID of a data integration partner. The dropdown list of this field displays partner IDs for the past thirty days.  It must be used together with \"project\" field to uniquely identify a data integration target.  For example, if a continuous AQuA session is to retrieve data incrementally for a Salesforce.com Org 00170000011K3Ub, you can use partner as \"Salesforce\", and \"project\" as \"00170000011K3Ub.\"   This field is required only if you are using AQuA in stateful mode. Otherwise, if you are using AQuA in stateless mode, partner field can be null.  **Note**: Zuora highly recommends you use the stateless mode instead of the stateful mode to extract bulk data. See <a href=\"https://knowledgecenter.zuora.com/Zuora_Central_Platform/API/AB_Aggregate_Query_API/Bulk_data__extraction_from_Zuora_using_AQuA\" target=\"_blank\">Bulk data extraction from Zuora using AQuA</a> for best practices.  **Note**: Submit a request at <a href=\"http://support.zuora.com\" target=\"_blank\">Zuora Global Support</a> to obtain a partner ID.   # noqa: E501

        :return: The partner of this SubmitBatchQueryResponse.  # noqa: E501
        :rtype: str
        """
        return self._partner

    @partner.setter
    def partner(self, partner):
        """Sets the partner of this SubmitBatchQueryResponse.

        The partner field indicates the unique ID of a data integration partner. The dropdown list of this field displays partner IDs for the past thirty days.  It must be used together with \"project\" field to uniquely identify a data integration target.  For example, if a continuous AQuA session is to retrieve data incrementally for a Salesforce.com Org 00170000011K3Ub, you can use partner as \"Salesforce\", and \"project\" as \"00170000011K3Ub.\"   This field is required only if you are using AQuA in stateful mode. Otherwise, if you are using AQuA in stateless mode, partner field can be null.  **Note**: Zuora highly recommends you use the stateless mode instead of the stateful mode to extract bulk data. See <a href=\"https://knowledgecenter.zuora.com/Zuora_Central_Platform/API/AB_Aggregate_Query_API/Bulk_data__extraction_from_Zuora_using_AQuA\" target=\"_blank\">Bulk data extraction from Zuora using AQuA</a> for best practices.  **Note**: Submit a request at <a href=\"http://support.zuora.com\" target=\"_blank\">Zuora Global Support</a> to obtain a partner ID.   # noqa: E501

        :param partner: The partner of this SubmitBatchQueryResponse.  # noqa: E501
        :type: str
        """

        self._partner = partner

    @property
    def project(self):
        """Gets the project of this SubmitBatchQueryResponse.  # noqa: E501

        The project field contains the unique ID of a data integration project for a particular partner. The dropdown list of this field displays project IDs for the past thirty days.  This field must be used together with partner field to uniquely identify a data integration target.   This field is required only if you are using AQuA in stateful mode. Otherwise, if you are using AQuA in stateless mode, partner field can be null.   # noqa: E501

        :return: The project of this SubmitBatchQueryResponse.  # noqa: E501
        :rtype: str
        """
        return self._project

    @project.setter
    def project(self, project):
        """Sets the project of this SubmitBatchQueryResponse.

        The project field contains the unique ID of a data integration project for a particular partner. The dropdown list of this field displays project IDs for the past thirty days.  This field must be used together with partner field to uniquely identify a data integration target.   This field is required only if you are using AQuA in stateful mode. Otherwise, if you are using AQuA in stateless mode, partner field can be null.   # noqa: E501

        :param project: The project of this SubmitBatchQueryResponse.  # noqa: E501
        :type: str
        """

        self._project = project

    @property
    def status(self):
        """Gets the status of this SubmitBatchQueryResponse.  # noqa: E501

        The status of the batch execution: - pending: The query was validated and submitted to the query executor for processing. - executing: The query is being processed. - completed: The query was successfully executed. - aborted: The query execution failed. - cancelled: The query was not processed.   # noqa: E501

        :return: The status of this SubmitBatchQueryResponse.  # noqa: E501
        :rtype: str
        """
        return self._status

    @status.setter
    def status(self, status):
        """Sets the status of this SubmitBatchQueryResponse.

        The status of the batch execution: - pending: The query was validated and submitted to the query executor for processing. - executing: The query is being processed. - completed: The query was successfully executed. - aborted: The query execution failed. - cancelled: The query was not processed.   # noqa: E501

        :param status: The status of this SubmitBatchQueryResponse.  # noqa: E501
        :type: str
        """
        allowed_values = ["pending", "executing", "completed", "aborted", "cancelled"]  # noqa: E501
        if (self._configuration.client_side_validation and
                status not in allowed_values):
            raise ValueError(
                "Invalid value for `status` ({0}), must be one of {1}"  # noqa: E501
                .format(status, allowed_values)
            )

        self._status = status

    @property
    def use_last_completed_job_queries(self):
        """Gets the use_last_completed_job_queries of this SubmitBatchQueryResponse.  # noqa: E501

        If this flag is set to `true`, then all the previous queries are merged with existing queries.  If the flag is set to `false`, then the previous queries are ignored, and only the new query is executed.   # noqa: E501

        :return: The use_last_completed_job_queries of this SubmitBatchQueryResponse.  # noqa: E501
        :rtype: bool
        """
        return self._use_last_completed_job_queries

    @use_last_completed_job_queries.setter
    def use_last_completed_job_queries(self, use_last_completed_job_queries):
        """Sets the use_last_completed_job_queries of this SubmitBatchQueryResponse.

        If this flag is set to `true`, then all the previous queries are merged with existing queries.  If the flag is set to `false`, then the previous queries are ignored, and only the new query is executed.   # noqa: E501

        :param use_last_completed_job_queries: The use_last_completed_job_queries of this SubmitBatchQueryResponse.  # noqa: E501
        :type: bool
        """

        self._use_last_completed_job_queries = use_last_completed_job_queries

    @property
    def version(self):
        """Gets the version of this SubmitBatchQueryResponse.  # noqa: E501

        The API version you want to use.   The supported versions are as follows:   - `1.1`. It supports both modes   - `1.0`. Default. It supports stateless modes only.  See <a href=\"https://knowledgecenter.zuora.com/Zuora_Central_Platform/API/AB_Aggregate_Query_API/BA_Stateless_and_Stateful_Modes\" target=\"_blank\">Stateless and stateful modes</a> for more information.   # noqa: E501

        :return: The version of this SubmitBatchQueryResponse.  # noqa: E501
        :rtype: float
        """
        return self._version

    @version.setter
    def version(self, version):
        """Sets the version of this SubmitBatchQueryResponse.

        The API version you want to use.   The supported versions are as follows:   - `1.1`. It supports both modes   - `1.0`. Default. It supports stateless modes only.  See <a href=\"https://knowledgecenter.zuora.com/Zuora_Central_Platform/API/AB_Aggregate_Query_API/BA_Stateless_and_Stateful_Modes\" target=\"_blank\">Stateless and stateful modes</a> for more information.   # noqa: E501

        :param version: The version of this SubmitBatchQueryResponse.  # noqa: E501
        :type: float
        """

        self._version = version

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
        if issubclass(SubmitBatchQueryResponse, dict):
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
        if not isinstance(other, SubmitBatchQueryResponse):
            return False

        return self.to_dict() == other.to_dict()

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        if not isinstance(other, SubmitBatchQueryResponse):
            return True

        return self.to_dict() != other.to_dict()
