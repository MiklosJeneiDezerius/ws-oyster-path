# Oyster Bank | Customer Spend Analytics & Debit Card Tier Alignment

## The Oyster Bank story

A customer opens an account with Oyster Bank and uses it for everyday transactions. A customer can hold more than one account, so the bank evaluates spending separately for each account rather than combining it across the customer. The debit card's assigned tier - Standard, Gold, or Platinum - is distinct from the customer's segment and the account product.

The bank wants to know whether the assigned debit card tier still fits how the account is used. A customer may be spending at a level that justifies an upgrade offer, or an account may have a premium card whose tier is higher than its recent spending would suggest. Neither case is an instruction to change the card. The purpose of the analysis is to identify where a review is worthwhile.

## How tier alignment is assessed

For each account and snapshot date, the model sums the recorded transaction amounts for that date and the preceding 29 calendar days. It compares the resulting **rolling 30-day account spend** with the tier of the debit card active on the snapshot date. The calculation uses account-level transaction amounts; it is not a merchant-category analysis or a measure of card revenue.

| Calculated spending tier | Default rolling 30-day spend |
|---|---:|
| Standard | Below 2,000 |
| Gold | At least 2,000 and below 5,000 |
| Platinum | At least 5,000 |

The thresholds are configurable demo values, expressed in the same units as the source transaction amounts. This calculation does not perform currency conversion; the thresholds and currency policy would need business approval before operational use.

- **Aligned:** The assigned tier matches the calculated spending tier.
- **Upgrade candidate:** The calculated tier is higher than the assigned tier, including a direct move from Standard to Platinum.
- **Downgrade candidate:** The calculated tier is lower than the assigned tier, including a direct move from Platinum to Standard.

Accounts without an active card, with a missing tier, or with an unrecognized tier are not classified as upgrade or downgrade candidates. The model also flags limited transaction history so a low-spend signal can be reviewed in context.

## What management sees in Power BI

The report uses the **latest available snapshot date in the selected date range**. The date range selects the date of the assessment; it does not redefine the 30-day spending window or add rolling totals across days. In the reporting model, **Total Cardholders** counts distinct accounts with an active card and a recorded tier, not distinct customers.

The [documented report design](bronze-development-walkthrough.md#report-layout-and-expected-results-as-of-2026-11-09) includes KPI cards for active cardholder accounts, the tier mismatch rate, and upgrade candidates. The mismatch rate is the share of counted accounts flagged as misaligned. A 100% stacked bar compares alignment by **assigned card tier**, while a donut shows the **calculated spending tiers** of misaligned accounts. The model also measures downgrade candidates; a separate KPI card for them is recommended in the report design.

The documented demo snapshot for **9 November 2026** shows **102** active cardholder accounts, of which **59 (57.8%)** were misaligned: **27** upgrade candidates and **32** downgrade candidates. These are sample-data results, not estimates of actual revenue or cost impact.

## What management can do with the results

| Business objective | How the report supports the decision |
|---|---|
| **Upselling and retention** | Prioritize accounts for an upgrade offer when recent spending supports a higher tier. Review product eligibility and customer needs before making the offer. |
| **Premium benefit cost control** | Review premium-tier accounts whose spending supports a lower tier. A low-spend flag is a starting point for investigation, not evidence that the bank is losing money. Assessing profitability requires fee, revenue, and benefit-cost data. |
| **Product policy review** | Compare mismatch rates across assigned tiers to identify where the bank should investigate whether its thresholds or card assignment criteria still fit customer behavior. |

The solution **flags candidates for review**. It does not change card tiers, convert account products, or contact customers automatically.
