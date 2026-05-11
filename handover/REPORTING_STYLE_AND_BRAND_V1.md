# Reporting Style and Brand v1

## 1. Reporting Objective
Produce reports that are decision-oriented, technically defensible, and clearly tied to business impact.

## 2. Narrative Rules
1. Lead with business risk and operational consequence
2. Keep language concrete; avoid speculative claims
3. Map every recommendation to owner, timeline, and expected outcome
4. Separate verified facts from inferred hypotheses

## 3. Required Section Order
1. Executive summary
2. Readiness score
3. Top business risks
4. Source inventory and freshness
5. Data quality scorecard
6. Relationship and KPI candidate map
7. Seven-day action plan
8. Technical appendix

## 4. Visualization Standards
1. Trend metrics: line chart
2. Category comparison: horizontal bar chart
3. Distribution and outliers: histogram/box plot
4. Completeness by field: heatmap
5. Relationship confidence: tabular matrix

## 5. Business Impact Framing
Every issue should include:
1. Risk statement
2. KPI impact statement
3. Remediation action
4. Time-to-fix estimate
5. Owner role

Use `handover/templates/impact_rules.yaml` to standardize phrasing.

## 6. Brand Usage
1. Apply theme values from `handover/templates/brand.yaml`
2. Follow section and voice configuration from `handover/templates/report_template.yaml`
3. Do not introduce unapproved colors or fonts
4. Ensure text contrast meets readability standards

## 7. Review Checklist
1. No sensitive values exposed
2. All scores traceable to formula/rules
3. KPI candidates include confidence and blockers
4. Action plan items are specific and dated
5. Brand tokens are applied consistently
