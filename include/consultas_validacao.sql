-- Snapshot atual
SELECT *
FROM pricing_categoria_snapshot
ORDER BY data_referencia DESC, categoria;

-- Historico por execucao
SELECT dag_run_id, data_referencia, categoria, preco_medio,
       preco_minimo, preco_maximo, quantidade_produtos, criado_em
FROM pricing_categoria_historico
ORDER BY criado_em DESC, categoria;

-- Prova de ausencia de duplicidade no snapshot
SELECT data_referencia, categoria, COUNT(*)
FROM pricing_categoria_snapshot
GROUP BY data_referencia, categoria
HAVING COUNT(*) > 1;
