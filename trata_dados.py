import pandas as pd
import os


class ReconciliationTransformer:
    def __init__(self, caminho, razao, competencia):
        self.competencia = competencia
        self.razao = razao
        self.df_raw = pd.read_csv(caminho, sep=";")
        self.df = self.df_raw.copy()
        self.gera_final()

    def gera_final(self):
        arquivo_tratado = self.transformar()
        downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")
        os.makedirs(downloads_path, exist_ok=True)
        nome_arquivo = f"dados_tratados_{self.razao}_{self.competencia}.xlsx"
        full_path = os.path.join(downloads_path, nome_arquivo)
        arquivo_tratado.to_excel(full_path, index=False)
        print(f"Arquivo salvo com sucesso em: {full_path}")

    def transformar(self):
        self.df = self.df.rename(columns={
            'loja_id_curto': 'Id Loja',
            'pedido_associado_ifood_curto': 'Pedido Associado',
            'competencia': 'Competência',
            'fato_gerador': 'Fator',
            'descricao_lancamento': 'Descrição',
            'valor': 'Valor',
            'base_calculo': 'Base Cálculo',
            'percentual_taxa': 'Percentual Taxa',
            'motivo_cancelamento': 'Motivo Cancelamento'
        })
        self.df = self.df[self.df['Motivo Cancelamento'].isna()]

        self.df['Descrição'] = self.df['Descrição'].str.strip()
        self.df['Fator'] = self.df['Fator'].str.strip()

        base_cols = ['Id Loja', 'Pedido Associado', 'Competência', 'Fator']
        df_base = self.df[base_cols].drop_duplicates().reset_index(drop=True)

        def extrair_valor(coluna_origem, descricao, forcar_positivo=False):
            df_filtrado = self.df[self.df['Descrição'] == descricao].copy()
            if forcar_positivo:
                df_filtrado.loc[:, coluna_origem] = df_filtrado[coluna_origem].abs()
            return df_filtrado[base_cols + [coluna_origem]].rename(columns={coluna_origem: descricao})

        df_percen_taxa_trans = extrair_valor('Percentual Taxa', 'Taxa de transação', forcar_positivo=True)
        df_base_taxa_trans = extrair_valor('Base Cálculo', 'Taxa de transação', forcar_positivo=True)
        df_taxa_trans = extrair_valor('Valor', 'Taxa de transação', forcar_positivo=True)
        df_taxa_ifood = extrair_valor('Valor', 'Taxa de serviço iFood cobrada do cliente', forcar_positivo=True)
        df_promocao = extrair_valor('Valor', 'Promoção custeada pelo iFood', forcar_positivo=True)
        df_entrega = extrair_valor('Valor', 'Taxa entrega iFood', forcar_positivo=True)
        df_percentual_entrega = extrair_valor('Percentual Taxa', 'Comissão do iFood (entrega iFood)',
                                              forcar_positivo=True)
        df_base_comissao = extrair_valor('Base Cálculo', 'Comissão do iFood (entrega iFood)', forcar_positivo=True)
        df_comissao_ifood = extrair_valor('Valor', 'Comissão do iFood (entrega iFood)', forcar_positivo=True)
        df_valor_produto = extrair_valor('Base Cálculo', 'Comissão do iFood (entrega iFood)')
        df_valor_pago = extrair_valor('Valor', 'Entrada Financeira')

        def agrupar_valor_cancelado(fator, descricao, coluna_origem, nome_final):
            df_filtrado = self.df[
                (self.df['Fator'] == fator) &
                (self.df['Descrição'] == descricao)
                ].copy()

            df_filtrado[coluna_origem] = df_filtrado[coluna_origem].abs()

            return (
                df_filtrado
                .groupby(['Id Loja', 'Pedido Associado', 'Competência'], as_index=False)[coluna_origem]
                .sum()
                .rename(columns={coluna_origem: nome_final})
            )

        df_produto_cancelado = agrupar_valor_cancelado(
            fator='Cancelamento Parcial',
            descricao='Entrada Financeira',
            coluna_origem='Valor',
            nome_final='Produto Cancelado'
        )

        df_taxa_trans_cancelada = agrupar_valor_cancelado(
            fator='Cancelamento Parcial',
            descricao='Taxa de transação',
            coluna_origem='Valor',
            nome_final='Taxa Transação Cancelado'
        )

        df_taxa_comissao_cancelada = agrupar_valor_cancelado(
            fator='Cancelamento Parcial',
            descricao='Comissão do iFood',
            coluna_origem='Valor',
            nome_final='Taxa Comissão Cancelado'
        )

        def merge(df_left, df_right, nome_final):
            colunas_comuns = [col for col in base_cols if col in df_right.columns]
            return df_left.merge(df_right, on=colunas_comuns, how='left').rename(columns={df_right.columns[-1]: nome_final})

        lista_merges = [
            (df_percen_taxa_trans, 'Taxa Transação %'),
            (df_base_taxa_trans, 'Base Cálculo Taxa'),
            (df_taxa_trans, 'Taxa de Transação'),
            (df_taxa_ifood, 'Taxa Serviço'),
            (df_promocao, 'Promoção Custeada IFOOD'),
            (df_entrega, 'Taxa Entrega Ifood'),
            (df_percentual_entrega, 'Taxa (Entrega) %'),
            (df_base_comissao, 'Base Cálculo Comissão'),
            (df_comissao_ifood, 'Comissão Ifood (Entrega Ifood)'),
            (df_valor_pago, 'Valor Pago'),
            (df_valor_produto, 'Valor Produtos')
        ]
        for df_temp, nome_coluna_final in lista_merges:
            df_base = merge(df_base, df_temp, nome_coluna_final)

        df_base.fillna(0.0, inplace=True)

        df_base['Valor Bruto'] = (
                df_base['Valor Produtos'] +
                df_base['Taxa Entrega Ifood'] +
                df_base['Taxa Serviço']
        )

        df_base['Valor a Receber'] = (
                df_base['Valor Bruto'] -
                df_base['Taxa Entrega Ifood'] -
                df_base['Taxa Serviço'] -
                df_base['Taxa de Transação'] -
                df_base['Comissão Ifood (Entrega Ifood)']
        )
        df_base['Valor Bruto'] = df_base['Valor Bruto'].round(2)
        df_base['Valor a Receber'] = df_base['Valor a Receber'].round(2)
        colunas_modelo = [
            'Id Loja',
            'Pedido Associado',
            'Competência',
            'Fator',
            'Valor Produtos',
            'Valor Bruto',
            'Valor a Receber',
            'Valor Pago',
            'Comissão Ifood (Entrega Ifood)',
            'Base Cálculo Comissão',
            'Taxa (Entrega) %',
            'Taxa Entrega Ifood',
            'Promoção Custeada IFOOD',
            'Taxa Serviço',
            'Taxa de Transação',
            'Base Cálculo Taxa',
            'Taxa Transação %'
        ]

        for col in colunas_modelo:
            if col not in df_base.columns:
                df_base[col] = 0.0

        df_base = df_base[df_base['Valor Produtos'] > 0]
        df_final = df_base[colunas_modelo]
        return df_final
