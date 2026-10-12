CREATE OR REFRESH MATERIALIZED VIEW test_exp_bricks.testdata.credit_card_gold AS
SELECT CreditCardID, CardType, CardNumber, ExpMonth, ExpYear, ModifiedDate
FROM test_exp_bricks.testdata.credit_card_silver