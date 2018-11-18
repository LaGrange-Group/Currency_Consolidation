import MySQLdb, telegram

conn=MySQLdb.connect(host='localhost', user='root',passwd='###', db='datadb')
cursor = conn.cursor()

class Consolidation:

    def scan_for_consolidation():
        consolidation_scan_query = """INSERT INTO consolidation (TOKEN)
                                        SELECT A.TICKER AS TOKEN

                                        	FROM
                                        		(SELECT TICKER, PRICE
                                        		FROM storage
                                        		WHERE BATCH_KEY = (SELECT (MAX(BATCH_KEY) - 5060) FROM batch)) A

                                        		INNER JOIN (SELECT TICKER, MAX(PRICE) AS MAX_PRICE
                                        		FROM storage
                                                WHERE BATCH_KEY BETWEEN (SELECT (MAX(BATCH_KEY) - 5060) FROM batch) AND (SELECT MAX(BATCH_KEY) FROM batch)
                                        		GROUP BY TICKER) B
                                        		ON A.TICKER = B.TICKER

                                        		INNER JOIN (SELECT TICKER, MIN(PRICE) AS MIN_PRICE
                                        		FROM storage
                                                WHERE BATCH_KEY BETWEEN (SELECT (MAX(BATCH_KEY) - 5060) FROM batch) AND (SELECT MAX(BATCH_KEY) FROM batch)
                                        		GROUP BY TICKER) C
                                        		ON A.TICKER = C.TICKER

                                        WHERE B.MAX_PRICE < (A.PRICE * 0.02 + A.PRICE)
                                        AND C.MIN_PRICE > ((A.PRICE * 0.02 + (A.PRICE * -1)) * -1)
                                        AND A.TICKER NOT IN ('BCCBTC')"""
