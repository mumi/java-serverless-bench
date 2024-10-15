# Troubleshooting

###  "The operation is not permitted for namespace 'Microsoft.OperationalInsights'"
This error occurs when the subscription is not eligible for the Microsoft.OperationalInsights Resource provider, which is required for the deployment.
This is for example the case for the free Azure for Students Starter subscription. To resolve this issue, you can upgrade your subscription to a Pay-As-You-Go subscription.

### No app insights for the function
If you don't see any app insights for the function, this usually means, that the region doesn't support the legacy api to enable app insights, which is used by the underlying azure library to deploy the function. 
To resolve this issue, you can deploy the function in a different region, or enable app insights manually in the azure portal.