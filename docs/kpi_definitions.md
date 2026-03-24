# KPI Definitions

This reference defines the key performance indicators (KPIs) used throughout the synthetic analytics ecosystem.  The definitions draw upon established industry practice as well as published guidelines.

## Active users

- **Daily Active Users (DAU)** – the number of unique members who generate at least one event on a given calendar day.  DAU is a fundamental engagement indicator and reflects the immediacy of product usage.
- **Weekly Active Users (WAU)** – the count of unique members active at least once during a seven‑day rolling window.
- **Monthly Active Users (MAU)** – the count of unique members active at least once in a 30‑day window.  The ratio of DAU to MAU is often used to gauge stickiness.

## Retention metrics

- **Retention rate (D7, D30, D90)** – the percentage of a sign‑up cohort that remains active seven, thirty or ninety days after signing up.  These intervals correspond to common retention measurements.  Retention is calculated by dividing the number of members who generate an event within the interval by the total number of members in the cohort.
- **Churn rate** – the complement of retention, representing the proportion of members who become inactive during a period.  The synthetic generator models higher churn in months two and three to reflect typical behaviour for new users.

## Feature adoption

- **Unique feature users** – the number of members who interact with a particular feature during a defined period.  Weekly metrics provide insights into adoption and saturation.
- **New adopters** – members whose first interaction with a feature occurs in the current period.  This group quantifies new adoption.
- **Repeat users** – members who have previously used the feature and continue to use it in the current period.  High repeat usage signals sustained engagement.

## Experimentation metrics

- **Conversion rate** – the proportion of members in a variant who achieve a defined success event (e.g. completing onboarding).  Conversions are compared between variants using t‑tests.
- **Lift** – the relative improvement of the treatment group over the control group.  If the difference in means is statistically significant at the 95 % level (p < 0.05), the result is considered actionable.
- **Cohen’s d** – an effect size that standardises the difference in means by the pooled standard deviation.  Values around 0.2, 0.5 and 0.8 are interpreted as small, medium and large effects, respectively.

## Health KPIs

- **Heart rate variability (HRV)** – the variation in time between successive heartbeats.  Higher HRV generally indicates better cardiovascular fitness and recovery.
- **Resting heart rate** – the average number of heart beats per minute at rest.  Lower values are associated with better fitness.
- **Sleep hours & quality** – total hours slept per day and a quality score from 0 to 100.  Adequate sleep is a leading indicator of recovery and performance.
- **Strain & recovery** – composite scores derived from multiple sensors measuring exertion and restorative capacity.

These KPIs support a range of analyses, from basic engagement tracking to advanced cohort retention and experimental measurement.  Establishing clear definitions upfront ensures consistency across dashboards, models and tests.