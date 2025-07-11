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

        base_cols = ['Id Loja', 'Pedido Associado', 'Competência']
        df_base = self.df[base_cols].drop_duplicates().reset_index(drop=True)

        def extrair_valor(coluna_origem, descricao, nome_final, forcar_positivo=False):
            df_filtrado = self.df[self.df['Descrição'] == descricao].copy()
            if forcar_positivo:
                df_filtrado[coluna_origem] = df_filtrado[coluna_origem].abs()
            return (
                df_filtrado[base_cols + [coluna_origem]]
                .groupby(base_cols)
                .sum()
                .reset_index()
                .rename(columns={coluna_origem: nome_final})
            )

        lista_campos = [
            ('Valor', 'Entrada Financeira', 'Valor Pago'),
            ('Base Cálculo', 'Comissão do iFood (entrega própria da loja)', 'Base'),
            ('Valor', 'Taxa de transação', 'Taxa de Transação', True),
            ('Valor', 'Taxa de serviço iFood cobrada do cliente', 'Taxa Serviço', True),
            ('Valor', 'Comissão do iFood (entrega própria da loja)', 'Comissão Ifood', True),
            ('Valor', 'Promoção custeada pelo iFood', 'Promoção Custeada IFOOD', True),
            ('Valor', 'Promoção custeada pela loja', 'Promoção Loja', True),
            ('Valor', 'Promoção custeada pela loja no delivery', 'Promoção Loja no Delivery', True),
        ]

        for args in lista_campos:
            df_temp = extrair_valor(*args)
            df_base = df_base.merge(df_temp, on=base_cols, how='left')

        df_base.fillna(0.0, inplace=True)

        df_base['Valor Bruto'] = (
                df_base['Base'] +
                df_base['Taxa Serviço'] +
                df_base['Promoção Loja'] +
                df_base['Promoção Loja no Delivery']
        )

        df_base['Valor a Receber'] = (
                df_base['Valor Bruto'] -
                df_base['Taxa de Transação'] -
                df_base['Comissão Ifood'] -
                df_base['Promoção Loja'] -
                df_base['Promoção Loja no Delivery'] -
                df_base['Taxa Serviço']
        )

        df_base['Valor Bruto'] = df_base['Valor Bruto'].round(2)
        df_base['Valor a Receber'] = df_base['Valor a Receber'].round(2)

        colunas_modelo = [
            'Id Loja', 'Pedido Associado', 'Competência', 'Valor a Receber', 'Valor Bruto',
            'Valor Pago', 'Comissão Ifood', 'Taxa Serviço', 'Taxa de Transação',
            'Promoção Loja', 'Promoção Loja no Delivery', 'Promoção Custeada IFOOD'
        ]

        for col in colunas_modelo:
            if col not in df_base.columns:
                df_base[col] = 0.0

        df_base = df_base[df_base['Base'] > 0]
        df_final = df_base[colunas_modelo]
        return df_final
