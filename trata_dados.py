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
            'pedido_associado_ifood': 'Pedido UUID',
            'pedido_associado_ifood_curto': 'Pedido Curto',
            'competencia': 'Competência',
            'fato_gerador': 'Fator',
            'data_fato_gerador': 'Data Pedido',
            'descricao_lancamento': 'Descrição',
            'valor': 'Valor',
            'base_calculo': 'Base Cálculo',
            'percentual_taxa': 'Percentual Taxa',
            'motivo_cancelamento': 'Motivo Cancelamento'
        })

        self.df['Data Pedido'] = pd.to_datetime(self.df['Data Pedido'], errors='coerce').dt.strftime('%Y-%m-%d')
        self.df = self.df[self.df['Motivo Cancelamento'].isna()]
        self.df['Descrição'] = self.df['Descrição'].str.strip()
        self.df['Fator'] = self.df['Fator'].str.strip()

        base_cols = ['Id Loja', 'Pedido UUID', 'Competência']
        df_base = self.df[base_cols + ['Pedido Curto']].drop_duplicates().reset_index(drop=True)

        def extrair_valor(coluna_origem, descricao=None, descricao_alt=None,
                          nome_final=None, forcar_positivo=False, fato_gerador=None):
            df_filtrado = self.df.copy()

            if fato_gerador:
                if isinstance(fato_gerador, (list, tuple)):
                    cond = df_filtrado['Fator'].str.lower().apply(lambda x: any(fg.lower() in x for fg in fato_gerador if isinstance(x, str)))
                else:
                    cond = df_filtrado['Fator'].str.lower().str.contains(fato_gerador.lower(), na=False, regex=False)
                df_filtrado = df_filtrado[cond]

            if descricao:
                if isinstance(descricao, (list, tuple)):
                    cond = df_filtrado['Descrição'].str.lower().apply(
                        lambda x: any(d.lower() in x for d in descricao if isinstance(x, str)))
                else:
                    cond = df_filtrado['Descrição'].str.strip().str.lower() == descricao.lower().strip()

                if descricao_alt:
                    if isinstance(descricao_alt, (list, tuple)):
                        cond |= df_filtrado['Descrição'].str.lower().apply(
                            lambda x: any(d.lower() in x for d in descricao_alt if isinstance(x, str)))
                    else:
                        cond |= df_filtrado['Descrição'].str.lower().str.contains(descricao_alt.lower(), na=False, regex=False)

                df_filtrado = df_filtrado[cond]

            if forcar_positivo:
                df_filtrado[coluna_origem] = df_filtrado[coluna_origem].abs()

            if coluna_origem == 'Data Pedido':
                df_filtrado[coluna_origem] = pd.to_datetime(df_filtrado[coluna_origem], errors='coerce')
                resultado = df_filtrado.groupby(base_cols)[coluna_origem].min().reset_index()
                resultado[coluna_origem] = resultado[coluna_origem].dt.strftime('%Y-%m-%d')
            else:
                resultado = df_filtrado[base_cols + [coluna_origem]].groupby(base_cols).sum().reset_index()

            return resultado

        lista_campos = [
            ('Data Pedido', 'Entrada Financeira', None, 'Data pedido'),
            ('Valor', 'Entrada Financeira', None, 'Valor Pago'),
            ('Base Cálculo', 'Comissão do iFood (entrega própria da loja)', 'Comissão do iFood (entrega iFood)', 'Base', True),
            ('Valor', 'Taxa de transação', None, 'Taxa de Transação', True),
            ('Valor', 'Taxa de serviço iFood cobrada do cliente', None, 'Taxa Serviço', True),
            ('Valor',
             ['Comissão do iFood', 'Comissão do iFood (entrega própria da loja)', 'Comissão do iFood (entrega iFood)'],
             None,
             'Comissão Ifood', True),
            ('Valor', 'Taxa entrega iFood', None, 'Taxa entrega iFood', True),
            ('Valor', 'Promoção custeada pelo iFood', None, 'Promoção Custeada IFOOD', True),
            ('Valor', 'Promoção custeada pela loja', None, 'Promoção Loja', True),
            ('Valor', 'Promoção custeada pela loja no delivery', None, 'Promoção Loja no Delivery', True),
        ]

        for coluna_origem, descricao, descricao_alt, nome_final, *rest in lista_campos:
            forcar_positivo = rest[0] if rest else False
            df_temp = extrair_valor(coluna_origem, descricao, descricao_alt, nome_final,
                                    forcar_positivo, fato_gerador=['Venda', 'Solicitacao frete'])
            df_temp = df_temp.rename(columns={coluna_origem: nome_final})
            df_base = df_base.merge(df_temp, on=base_cols, how='left')

        def extrair_valor_cancelamento(coluna_origem, fato_gerador=None, descricao=None, forcar_positivo=False):
            df_filtrado = df_cancelamento.copy()

            if fato_gerador:
                df_filtrado = df_filtrado[
                    df_filtrado['Fator'].str.lower().str.contains(fato_gerador.lower(), na=False, regex=False)
                ]

            if descricao:
                df_filtrado = df_filtrado[
                    df_filtrado['Descrição'].str.lower().str.contains(descricao.lower(), na=False, regex=False)
                ]

            if forcar_positivo:
                df_filtrado[coluna_origem] = df_filtrado[coluna_origem].abs()

            return (
                df_filtrado[base_cols + [coluna_origem]]
                .groupby(base_cols)
                .sum()
                .reset_index()
            )

        pedidos_com_ressarcimento = self.df[
            self.df['Fator'].str.lower().str.contains('ressarcimento', na=False)
        ]['Pedido UUID'].unique()

        df_cancelamento = self.df[
            ~self.df['Pedido UUID'].isin(pedidos_com_ressarcimento)
        ].copy()

        lista_campos_cancelamento = [
            ('Valor', 'Cancelamento Parcial', 'Entrada Financeira', 'Valor Pago Cancelado'),
            ('Valor', 'Cancelamento Parcial', 'Comissão do iFood', 'Comissão Ifood Cancelada'),
            ('Valor', 'Cancelamento Parcial', 'Taxa de transação', 'Taxa de Transação Cancelada')
        ]

        for coluna_origem, fator, descricao, nome_final in lista_campos_cancelamento:
            df_temp = extrair_valor_cancelamento(coluna_origem, fator, descricao, forcar_positivo=True)
            df_temp = df_temp.rename(columns={coluna_origem: nome_final})
            df_base = df_base.merge(df_temp, on=base_cols, how='left')

        df_base.fillna(0.0, inplace=True)

        df_base['Taxa de Transação'] = (
                df_base['Taxa de Transação'] - df_base['Taxa de Transação Cancelada']
        )

        df_base['Comissão Ifood'] = (
                df_base['Comissão Ifood'] - df_base['Comissão Ifood Cancelada']
        )

        df_base['Valor Bruto'] = (
                df_base['Valor Pago'] +
                df_base['Promoção Loja'] +
                df_base['Promoção Loja no Delivery'] +
                df_base['Promoção Custeada IFOOD'] -
                df_base['Valor Pago Cancelado']
        )

        df_base['Valor a Receber'] = (
                df_base['Valor Bruto'] -
                df_base['Taxa de Transação'] -
                df_base['Comissão Ifood'] -
                df_base['Promoção Loja'] -
                df_base['Promoção Loja no Delivery'] -
                df_base['Taxa Serviço'] -
                df_base['Taxa entrega iFood']
        )

        df_base['Valor Bruto'] = df_base['Valor Bruto'].round(2)
        df_base['Valor a Receber'] = df_base['Valor a Receber'].round(2)

        colunas_modelo = [
            'Id Loja', 'Pedido UUID', 'Pedido Curto',
            'Competência',
            'Data pedido',
            'Valor a Receber',
            'Valor Bruto',
            'Valor Pago',
            'Comissão Ifood',
            'Taxa Serviço',
            'Taxa de Transação',
            'Promoção Loja',
            'Promoção Loja no Delivery',
            'Promoção Custeada IFOOD',
            'Taxa entrega iFood',
            'Valor Pago Cancelado'
        ]

        for col in colunas_modelo:
            if col not in df_base.columns:
                df_base[col] = 0.0

        df_base = df_base[df_base['Valor Bruto'] > 0]
        df_final = df_base[colunas_modelo]
        return df_final
