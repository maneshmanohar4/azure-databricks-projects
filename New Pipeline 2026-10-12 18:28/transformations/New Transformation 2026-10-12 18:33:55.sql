CREATE OR REFRESH STREAMING TABLE test_exp_bricks.testdata.credit_card_silver AS
SELECT t.CreditCardID, t.CardType, t.CardNumber, t.ExpMonth, t.ExpYear, t.ModifiedDate
FROM STREAM(test_exp_bricks.testdata.credit_card_bronze) t