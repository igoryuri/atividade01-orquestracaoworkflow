from __future__ import annotations

from typing import Any, Sequence

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator


class ValidarProdutosOperator(BaseOperator):
    """Valida o schema dos produtos da API de catalogo antes do processamento.

    Regras:
    - campos obrigatorios: id, title, price, category;
    - price numerico e nao negativo;
    - category nao vazia.

    Recebe a lista de produtos (normalmente um XComArg de ``buscar_produtos``),
    falha com AirflowException se houver qualquer produto invalido e retorna a
    lista validada, que vira XCom automaticamente.
    """

    template_fields: Sequence[str] = ("produtos",)
    ui_color = "#e8f4fd"

    CAMPOS_OBRIGATORIOS = frozenset({"id", "title", "price", "category"})

    def __init__(self, *, produtos: Any, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.produtos = produtos

    def execute(self, context: Any) -> list[dict[str, Any]]:
        if not isinstance(self.produtos, list) or not self.produtos:
            raise AirflowException("Nenhum produto recebido para validacao.")

        validos: list[dict[str, Any]] = []
        erros: list[str] = []
        for indice, produto in enumerate(self.produtos):
            if not isinstance(produto, dict):
                erros.append(f"indice={indice}, erro=registro nao e um objeto")
                continue
            ausentes = self.CAMPOS_OBRIGATORIOS - produto.keys()
            if ausentes:
                erros.append(f"indice={indice}, campos_ausentes={sorted(ausentes)}")
                continue
            try:
                preco = float(produto["price"])
                if preco < 0:
                    raise ValueError("preco negativo")
                if not str(produto["category"]).strip():
                    raise ValueError("categoria vazia")
            except (TypeError, ValueError) as exc:
                erros.append(f"indice={indice}, erro={exc}")
                continue
            validos.append(produto)

        if erros:
            raise AirflowException("Produtos invalidos: " + "; ".join(erros[:10]))

        self.log.info("Schema validado: %s produtos validos.", len(validos))
        return validos
