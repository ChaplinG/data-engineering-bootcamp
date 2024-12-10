/* Simple Selection: List all vehicles with type "Compact SUV" and value less than 30,000.00 */
SELECT nome, tipo, valor
	FROM veiculos
	WHERE tipo = 'SUV Compacta' AND valor < 300000.00;

/* Simple Join: Display the names of customers and the names of dealerships where they made purchases */
SELECT c.cliente, con.concessionaria
	FROM clientes c
	JOIN concessionarias con ON c.id_concessionarias = con.id_concessionarias
	ORDER BY cliente ASC;

/* Counting and Grouping: Count how many salespeople exist in each dealership */
SELECT con.concessionaria, cod.id_concessionarias, COUNT(*) AS numero_vendedores
	FROM vendedores cod
	JOIN concessionarias con ON cod.id_concessionarias = con.id_concessionarias
	GROUP BY cod.id_concessionarias, con.id_concessionarias
	ORDER BY concessionaria ASC;

/* Subquery: Find the most expensive vehicles sold in each vehicle type */
SELECT tipo, MAX(valor) AS valor_maximo
	FROM veiculos
	GROUP BY tipo
	ORDER BY valor_maximo ASC;

/* Multi-Join: List the customer's name, the purchased vehicle, and the amount paid for all sales */
SELECT cl.cliente, v.nome AS veiculo, vd.valor_pago
	FROM vendas vd
	JOIN clientes cl ON vd.id_clientes = cl.id_clientes
	JOIN veiculos v ON vd.id_veiculos = v.id_veiculos
	ORDER BY veiculo ASC;

/* Filter with Aggregation: Identify the dealerships that sold more than 5 vehicles */
SELECT con.id_concessionarias, con.concessionaria, COUNT(*) AS total_vendas
	FROM vendas v
	JOIN concessionarias con ON con.id_concessionarias = v.id_concessionarias
	GROUP BY con.id_concessionarias, v.id_concessionarias
	HAVING COUNT(*) > 5
	ORDER BY total_vendas DESC;

/* Query with ORDER BY and LIMIT: List the 3 most expensive vehicles available */
SELECT nome, valor
	FROM veiculos
	ORDER BY valor DESC
	LIMIT 3;

/* Query with Dates: Select all vehicles added in the last month */
SELECT nome, data_inclusao
	FROM veiculos
	WHERE data_inclusao > CURRENT_TIMESTAMP - INTERVAL '1 month';

/* Outer Join: List all cities and any dealership in them, if applicable */
SELECT cid.cidade, con.concessionaria
	FROM cidades cid
	LEFT JOIN concessionarias con ON cid.id_cidades = con.id_cidades
	ORDER BY cid.cidade;

/* Query with Multiple Conditions: Find customers who bought "Hybrid Premium SUV" vehicles or vehicles with a value above 60,000.00 */
SELECT cli.cliente, veiculos.nome, veiculos.valor
	FROM vendas vd
	JOIN veiculos ON vd.id_veiculos = veiculos.id_veiculos
	JOIN clientes cli ON vd.id_clientes = cli.id_clientes
	WHERE veiculos.tipo = 'SUV Premium Híbrida' OR veiculos.valor > 60000.00
	ORDER BY veiculos.nome;