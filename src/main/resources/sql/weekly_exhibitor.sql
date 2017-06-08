	SELECT
		  convert_timezone('US/Pacific', trx.transaction_end_datetime)::DATE as day,
			DATE_PART(week, convert_timezone('US/Pacific', trx.transaction_end_datetime)) as week_num,
			ch.chain_name as Exhibitor,
			th.paymentech_terminal_id as TerminalID,
			th.pdi_id as theater_id,
		  th.theater_name + '-' + th.city_name + ', ' + th.state_code as theater_name,
		  COALESCE(SUM(CASE WHEN typ.transaction_type_name != 'Refund' THEN trx.item_count END),0) as Tickets_sold_no_refund,
		  COALESCE(SUM(CASE WHEN typ.transaction_type_name != 'Refund' THEN trx.ticket_sales_amount END),0) as Total_sales_revenue,
		  COALESCE(SUM(CASE WHEN typ.transaction_type_name = 'Refund' THEN (trx.gross_transaction_amount +
												trx.sales_fee_amount_for_returned_purchase) END),0) as Total_refunds_amt,
		  COALESCE(SUM(CASE WHEN typ.transaction_type_name != 'Refund' THEN trx.ticket_sales_amount END),0) -
						COALESCE(SUM(CASE WHEN typ.transaction_type_name = 'Refund' THEN (trx.gross_transaction_amount +
																								trx.sales_fee_amount_for_returned_purchase) END),0) as Net_total_sum
	FROM
			dim_chain ch
	JOIN
			fact_transaction trx ON (ch.chain_key = trx.chain_key)
	JOIN
			dim_transaction_type typ ON (trx.transaction_type_key = typ.transaction_type_key)
	JOIN
			dim_theater th ON (trx.theater_key = th.theater_key)
	WHERE
			trx.is_successful = TRUE
			AND typ.transaction_type_name != 'GiftCertificate Purchase'
		  AND convert_timezone('US/Pacific', trx.transaction_end_datetime)::DATE >= ?
			AND convert_timezone('US/Pacific', trx.transaction_end_datetime)::DATE <= ?
			AND ch.chain_id = ?
  GROUP BY
		  convert_timezone('US/Pacific', trx.transaction_end_datetime)::DATE,
			DATE_PART(week, convert_timezone('US/Pacific', trx.transaction_end_datetime)),
			ch.chain_name,
		  th.paymentech_terminal_id,
		  th.pdi_id,
			th.theater_name + '-' + th.city_name + ', ' + th.state_code
  ORDER BY day asc
  LIMIT 1;