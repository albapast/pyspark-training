import os
from home_retention.app import PATH_VAR
import pandas as pd
import numpy as np
from home_retention.app import load_file2 as l
from datetime import date
import unidecode
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, isnan


class Home:
    def home_bottle(name_file, test=False):
        spark = SparkSession \
            .builder \
            .appName("Python Spark SQL basic example") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()

        # DELETE VARIABLES: We delete not necessary variables.
        del_variables = ['poliza_duracion', 'poliza_entidad_legal',
                         'poliza_fecha_ultimo_reemplazo', 'poliza_motivo_cancelacion_codigo',
                         'poliza_motivo_cancelacion_desc', 'poliza_fecha_emision_anulacion',
                         'poliza_fecha_cambio_estado', 'poliza_fecha_cartera','poliza_canal', 'poliza_forma_pago_desc',
                         'audit_poliza_versiones','audit_poliza_estados', 'audit_poliza_tiene_ultima_situacion',
                         'audit_poliza_tiene_anulacion','cliente_sexo_desc',
                         'cliente_presencia_tabla_morosidad','cliente_robinson','mediador_responsable_comercial_codigo',
                         'mediador_dn','mediador_nombre','mediador_TCPEFIJU', 'mediador_provincia',
                          'hogar_codigo_unico', 'hogar_clase_vivienda_desc',
                         'hogar_ubicacion_desc','cliente_tcnacper_desc','cliente_tcnacres_desc','hogar_caracter_desc',
                         'hogar_uso_desc','garantia_continente_forma','cliente_feulaccl','mediador_segmento_codigo','mediador_tctiesin',
                         'mediador_tctiesin_desc']




        #   Read CSV file and transform to DF
        file_df = l.load_file_HOGAR_matrix(PATH_VAR.path_out, name_file)
        file_df_spark= spark.read.csv(PATH_VAR.path_out + name_file, header=True)
        cols = file_df_spark.columns  # list of all columns
        for col in cols:
            file_df_spark = file_df_spark.withColumn(col, when(file_df_spark[col] == "?", float('nan')).otherwise(
                file_df_spark[col]))


        print('Before preprocesing', file_df.shape)
        print('Before preprocesing spark', file_df_spark.count(), len(file_df_spark.columns))
        file_df = file_df.drop(del_variables, 1)
        file_df_spark= file_df_spark.drop(*del_variables)

        print("Se han reducido el número de columnas a", len(file_df.columns))
        print("Se han reducido el número de columnas a", len(file_df_spark.columns))


        # 1) Generamos dummies para clase de vivienda hogar_clase_vivienda_cod
        file_df['hogar_clase_vivienda_cod'] = file_df['hogar_clase_vivienda_cod'].fillna('not_found')
        file_df_spark=file_df_spark.withColumn('hogar_clase_vivienda_cod',
                                               when(isnan(file_df_spark.hogar_clase_vivienda_cod),
                                                    'not_found').otherwise(
                                                     file_df_spark.hogar_clase_vivienda_cod))


        print(file_df['hogar_clase_vivienda_cod'].unique())
        file_df_spark.select('hogar_clase_vivienda_cod').distinct().show()


        file_df['hogar_clase_vivienda_cod'] = file_df['hogar_clase_vivienda_cod'].map(str)
        file_df_spark= file_df_spark.withColumn('hogar_clase_vivienda_cod',file_df_spark.hogar_clase_vivienda_cod.cast("string"))



        #   --- All possible values that the variable "hogar_clase_vivienda_cod" has
        ''' PI
            UA
            UF
            AT
            RU
            PB
            CH
            CD   '''

        # 2) Capital continente
        # Creat a dummy to the missing value
        file_df['garantia_continente_capital_asegurado'] = file_df['garantia_continente_capital_asegurado'].map(float)
        file_df_spark = file_df_spark.withColumn('garantia_continente_capital_asegurado',file_df_spark.garantia_continente_capital_asegurado.cast("float"))
        print(file_df.dtypes)
        file_df_spark.printSchema()

        file_df.loc[file_df['garantia_continente_capital_asegurado'] == 0, 'garantia_continente_capital_asegurado'] = 'not_found'
        file_df_spark= file_df_spark.withColumn('garantia_continente_capital_asegurado',
                                                when(file_df_spark.garantia_continente_capital_asegurado == 0,
                                                     'not_found').otherwise(file_df_spark.garantia_continente_capital_asegurado))

        file_df['garantia_continente_capital_asegurado'] = file_df['garantia_continente_capital_asegurado'].fillna('not_found')
        file_df_spark = file_df_spark.withColumn('garantia_continente_capital_asegurado',
                                               when(isnan(file_df_spark.garantia_continente_capital_asegurado),
                                                    'not_found').otherwise(
                                                     file_df_spark.garantia_continente_capital_asegurado))
        # Unlike Pandas, PySpark doesn’t consider NaN values to be NULL. df.fillna only fill null values,
        # not NaN values.
        file_df_spark.filter(file_df_spark.garantia_continente_capital_asegurado.like('%not_found%')).show()


        file_df['d_capital_continente_not_found'] = pd.Series(0, index=file_df.index)
        file_df_spark=file_df_spark.withColumn('d_capital_continente_not_found', lit(0))

        file_df.loc[file_df['garantia_continente_capital_asegurado'] == 'not_found', 'd_capital_continente_not_found'] = 1
        file_df_spark= file_df_spark.withColumn('d_capital_continente_not_found',
                                                when(file_df_spark.garantia_continente_capital_asegurado.like(
                                                    '%not_found%'),1).otherwise(file_df_spark.d_capital_continente_not_found))


        file_df.loc[file_df['garantia_continente_capital_asegurado'] == 'not_found', 'garantia_continente_capital_asegurado'] = np.NaN
        file_df_spark = file_df_spark.withColumn('garantia_continente_capital_asegurado',
                                                 when(file_df_spark.garantia_continente_capital_asegurado.like(
                                                     '%not_found%'), float('nan')).otherwise(
                                                     file_df_spark.garantia_continente_capital_asegurado))
        file_df_spark.filter(file_df_spark.d_capital_continente_not_found == 1).show()

        file_df['hogar_capital_continente'] = file_df['garantia_continente_capital_asegurado'].map(float)
        file_df_spark= file_df_spark.withColumn('hogar_capital_continente',
                                               file_df_spark.garantia_continente_capital_asegurado.cast("float"))


        file_df = file_df.drop('garantia_continente_capital_asegurado', 1)
        file_df_spark= file_df_spark.drop('garantia_continente_capital_asegurado')
        print(file_df.head(10))
        file_df_spark.show()

        ###################################HASTA AQUÍ EN SPARK#########################################################

        # 3) Hogar capital contenido
        # Creamos una dummy para los no identificados
        file_df['garantia_contenido_capital_asegurado'] = file_df['garantia_contenido_capital_asegurado'].map(float)
        file_df.loc[file_df['garantia_contenido_capital_asegurado'] == 0, 'garantia_contenido_capital_asegurado'] = 'not_found'
        file_df['garantia_contenido_capital_asegurado'] = file_df['garantia_contenido_capital_asegurado'].fillna('not_found')

        file_df['d_capital_contenido_not_found'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['garantia_contenido_capital_asegurado'] == 'not_found', 'd_capital_contenido_not_found'] = 1
        file_df.loc[file_df['garantia_contenido_capital_asegurado'] == 'not_found', 'garantia_contenido_capital_asegurado'] = np.NaN
        file_df['hogar_capital_contenido'] = file_df['garantia_contenido_capital_asegurado'].map(float)
        file_df = file_df.drop('garantia_contenido_capital_asegurado', 1)

        # 4) M2
        # Creamos una dummy para los no identificados
        file_df['hogar_m2'] = file_df['hogar_m2'].map(float)
        file_df.loc[file_df['hogar_m2'] == 0, 'hogar_m2'] = 'not_found'
        file_df['hogar_m2'] = file_df['hogar_m2'].fillna('not_found')
        file_df['d_hogar_m2_not_found'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['hogar_m2'] == 'not_found', 'd_hogar_m2_not_found'] = 1
        file_df.loc[file_df['hogar_m2'] == 'not_found', 'hogar_m2'] = np.NaN
        file_df['hogar_m2'] = file_df['hogar_m2'].map(float)

        # 5) Anio Construccion
        # Reemplazar valores raros
        year_today = date.today().year
        file_df['hogar_anio_construccion'] = pd.to_numeric(file_df['hogar_anio_construccion'], errors='coerse')
        file_df.loc[file_df['hogar_anio_construccion'] > year_today, 'hogar_anio_construccion'] = np.nan
        file_df.loc[file_df['hogar_anio_construccion'] < 1600, 'hogar_anio_construccion'] = np.nan

        # Creamos una dummy para los no identificados
        file_df.loc[file_df['hogar_anio_construccion'] == 0, 'hogar_anio_construccion'] = 'not_found'
        file_df['hogar_anio_construccion'] = file_df['hogar_anio_construccion'].fillna('not_found')
        file_df['d_hogar_anio_construccion_not_found'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['hogar_anio_construccion'] == 'not_found', 'd_hogar_anio_construccion_not_found'] = 1
        file_df.loc[file_df['hogar_anio_construccion'] == 'not_found', 'hogar_anio_construccion'] = np.NaN
        file_df['hogar_anio_construccion'] = file_df['hogar_anio_construccion'].map(float)
        print('NULL anio de construccion', file_df['hogar_anio_construccion'].isnull().sum())
        print('anio construccion', file_df['hogar_anio_construccion'].head())


        # 6)Dummy para hogar_ubicacion
        '''The values are: N, U, D'''
        file_df['hogar_ubicacion_cod'] = file_df['hogar_ubicacion_cod'].fillna('not_found')
        #dummy_hogar = pd.get_dummies(file_df['hogar_ubicacion_cod'], prefix='d_hogar_ubicacion')
        #file_df = pd.concat([file_df, dummy_hogar], axis=1)
        #del dummy_hogar

        # 7) hogar caracter
        '''The values are: P, I'''
        file_df['hogar_caracter_cod'] = file_df['hogar_caracter_cod'].fillna('not_found')
        file_df['hogar_caracter_cod'] = file_df['hogar_caracter_cod'].map(str)

        file_df['d_hogar_caracter_P'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['hogar_caracter_cod'] == 'P', 'd_hogar_caracter_P'] = 1
        #file_df['hogar_caracter_cod'] = file_df['hogar_caracter_cod'].fillna('not_found')

        file_df['d_hogar_caracter_not_found'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['hogar_caracter_cod'] == 'not_found', 'd_hogar_caracter_not_found'] = 1

        #dummy_hogar = pd.get_dummies(file_df['hogar_caracter_cod'], prefix='d_hogar_caracter')
        #file_df = pd.concat([file_df, dummy_hogar], axis=1)
        #del dummy_hogar

        # 8) Dummy para hogar uso
        ''' The values of hogar uso: P, S, A '''
        file_df['hogar_uso_cod'] = file_df['hogar_uso_cod'].fillna('not_found')
        file_df['hogar_uso_cod'] = file_df['hogar_uso_cod'].map(str)

        #dummy_hogar = pd.get_dummies(file_df['hogar_uso_cod'], prefix='d_hogar_uso')
        #file_df = pd.concat([file_df, dummy_hogar], axis=1)
        #del dummy_hogar

        # 9) garantia_continente_tipo
        '''The values of hogar uso: Inmueble, Obras de reforma  '''
        file_df.loc[file_df['garantia_continente_tipo'] == 'Inmueble', 'garantia_continente_tipo'] = 'I'
        file_df.loc[file_df['garantia_continente_tipo'] == 'Obras de reforma', 'garantia_continente_tipo'] = 'O'
        file_df['garantia_continente_tipo'] = file_df['garantia_continente_tipo'].fillna('not_found')
        #dummy_hogar = pd.get_dummies(file_df['garantia_continente_tipo'], prefix='d_continente_tipo')
        #file_df = pd.concat([file_df, dummy_hogar], axis=1)
        #del dummy_hogar
        file_df['d_continente_tipo_I'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['garantia_continente_tipo'] == 'I', 'd_continente_tipo_I'] = 1

        #   10) Indicador de danos esteticos
        '''The values of hogar uso: S,N  '''
        file_df['d_indicador_danios_esteticos_S'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['garantia_continente_indicador_danios_esteticos'] == 'S', 'd_indicador_danios_esteticos_S'] = 1

        #   11) garantia_robo_cobertura_metalico_cajafuerte
        '''This is a numerical values'''

        file_df['garantia_robo_cobertura_metalico_cajafuerte'] = file_df['garantia_robo_cobertura_metalico_cajafuerte'].fillna('not_found')
        file_df['d_metalico_cajafuerte_not_found'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['garantia_robo_cobertura_metalico_cajafuerte'] == 'not_found', 'd_metalico_cajafuerte_not_found'] = 1
        file_df.loc[file_df['garantia_robo_cobertura_metalico_cajafuerte'] == 'not_found', 'garantia_robo_cobertura_metalico_cajafuerte'] = np.NaN
        file_df['garantia_robo_cobertura_metalico_cajafuerte'] = file_df['garantia_robo_cobertura_metalico_cajafuerte'].map(float)

        #   12) garantia_robo_joyas_capital
        '''This is a numerical values'''

        file_df['garantia_robo_joyas_capital'] = file_df['garantia_robo_joyas_capital'].fillna('not_found')
        file_df['d_joyas_capital_not_found'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['garantia_robo_joyas_capital'] == 'not_found', 'd_joyas_capital_not_found'] = 1
        file_df.loc[file_df['garantia_robo_joyas_capital'] == 'not_found', 'garantia_robo_joyas_capital'] = np.NaN
        file_df['garantia_robo_joyas_capital'] = file_df['garantia_robo_joyas_capital'].map(float)

        #   13) garantia_robo_joyas_capital_base
        '''This is a categorical values S,N,C,U '''

        file_df['garantia_robo_joyas_capital_base'] = file_df['garantia_robo_joyas_capital_base'].fillna('N')
        file_df['garantia_robo_joyas_capital_base'] = file_df['garantia_robo_joyas_capital_base'].map(str)

        #dummy_hogar = pd.get_dummies(file_df['garantia_robo_joyas_capital_base'], prefix='d_joyas_capital_base')
        #file_df = pd.concat([file_df, dummy_hogar], axis=1)
        #del dummy_hogar

        #   14) garantia_robo_objetos_capital_base
        '''This is a categorical values S,N,C,U '''

        file_df['garantia_robo_objetos_capital_base'] = file_df['garantia_robo_objetos_capital_base'].fillna('N')
        file_df['garantia_robo_objetos_capital_base'] = file_df['garantia_robo_objetos_capital_base'].map(str)

        #dummy_hogar = pd.get_dummies(file_df['garantia_robo_objetos_capital_base'], prefix='d_objetos_capital_base')
        #file_df = pd.concat([file_df, dummy_hogar], axis=1)
        #del dummy_hogar

        #   15) garantia_robo_objetos_capital
        '''This is a numerical values'''

        file_df['garantia_robo_objetos_capital'] = file_df['garantia_robo_objetos_capital'].fillna('not_found')
        file_df['d_objetos_capital_not_found'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['garantia_robo_objetos_capital'] == 'not_found', 'd_objetos_capital_not_found'] = 1
        file_df.loc[file_df['garantia_robo_objetos_capital'] == 'not_found', 'garantia_robo_objetos_capital'] = np.NaN
        file_df['garantia_robo_objetos_capital'] = file_df['garantia_robo_objetos_capital'].map(float)

        #   16) garantia_robo_objetos_otros
        '''This is a numerical values'''

        file_df['garantia_robo_objetos_otros'] = file_df['garantia_robo_objetos_otros'].fillna('not_found')
        file_df['d_objetos_otros_not_found'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['garantia_robo_objetos_otros'] == 'not_found', 'd_objetos_otros_not_found'] = 1
        file_df.loc[file_df['garantia_robo_objetos_otros'] == 'not_found', 'garantia_robo_objetos_otros'] = np.NaN
        file_df['garantia_robo_objetos_otros'] = file_df['garantia_robo_objetos_otros'].map(float)

        #   17) Indicador de RC
        '''Categorical values S,N'''
        file_df['d_indicador_rc'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['garantia_indicador_rc'] == 'S', 'd_indicador_rc'] = 1

        #   18) Indicador de DE: Danios electricos
        '''Categorical values S,N'''
        file_df['d_indicador_de'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['garantia_indicador_de'] == 'S', 'd_indicador_de'] = 1

        #   19) Indicador de DA: Danios agua
        '''Categorical values S,N'''
        file_df['d_indicador_da'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['garantia_indicador_da'] == 'S', 'd_indicador_da'] = 1

        #   20) Indicador de cristales
        '''Categorical values S,N'''
        file_df['d_indicador_cristales'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['garantia_indicador_cristales'] == 'S', 'd_indicador_cristales'] = 1

        #   21) Indicador Defensa Juridica
        '''Categorical values S,N'''
        file_df['d_indicador_defjur'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['garantia_indicador_defjur'] == 'S', 'd_indicador_defjur'] = 1

        #   22) Indicador Robo
        '''Categorical values S,N'''
        file_df['d_indicador_robo'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['garantia_indicador_robo'] == 'S', 'd_indicador_robo'] = 1

        #   23) Nationalituy: we create a variable if the customer is Spanish or not

        file_df['d_cliente_nacionality_spain'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['cliente_tcnacper'] == 'ESP', 'd_cliente_nacionality_spain'] = 1

        #   24) Residence: we create a variable if the customer is the residence is Spain or not

        file_df['d_cliente_residence_spain'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['cliente_tcnacres'] == 'ESP', 'd_cliente_residence_spain'] = 1

        #   25) Numero de seguridad

        file_df['d_garantia_indicador_rc'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['garantia_indicador_rc'] == 'S', 'd_garantia_indicador_rc'] = 1

        file_df['d_garantia_indicador_de'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['garantia_indicador_de'] == 'S', 'd_garantia_indicador_de'] = 1

        file_df['d_garantia_indicador_da'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['garantia_indicador_da'] == 'S', 'd_garantia_indicador_da'] = 1

        file_df['d_garantia_indicador_cristales'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['garantia_indicador_cristales'] == 'S', 'd_garantia_indicador_cristales'] = 1

        file_df['d_garantia_indicador_defjur'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['garantia_indicador_defjur'] == 'S', 'd_garantia_indicador_defjur'] = 1

        file_df['d_garantia_indicador_robo'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['garantia_indicador_robo'] == 'S', 'd_garantia_indicador_robo'] = 1

        file_df['num_seguridad'] = pd.Series(0, index=file_df.index)
        file_df['num_seguridad'] = file_df.d_garantia_indicador_rc + file_df.d_garantia_indicador_de + file_df.d_garantia_indicador_da + file_df.d_garantia_indicador_cristales + file_df.d_garantia_indicador_defjur + file_df.d_garantia_indicador_robo

        #   26) Cliente poliza hogar duracion maxima

        file_df['cliente_poliza_hogar_duracion_maxima_year'] = file_df[
                                                                   'cliente_poliza_hogar_duracion_maxima'].astype(
            float) / 12

        #   27) Age of the customer
        #   reemplazar valores raros

        year_today = date.today().year
        file_df['cliente_fenaccon2'] = (file_df['cliente_fenaccon'].astype(float) // 10000)
        file_df['poliza_fecha_cartera2'] = file_df['poliza_fecha_vencimiento_nat'] // 100   # Aqui tengo el AnoMEs de la cartera
        #file_df['cliente_age'] = file_df.apply(lambda x: naive_age(x['cliente_fenaccon2'], year_today), axis=1)
        file_df['cliente_age'] = file_df['cliente_fenaccon2'].apply(lambda x: Home.naive_age(x, year_today))


        file_df.loc[file_df['cliente_age'] == 0, 'cliente_age'] = 'not_found'
        file_df['d_cliente_age_not_found'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['cliente_age'] == 'not_found', 'd_cliente_age_not_found'] = 1
        file_df.loc[file_df['cliente_age'] == 'not_found', 'cliente_age'] = np.NaN
        file_df['cliente_age'] = file_df['cliente_age'].map(float)

        print('NULL edad del cliente', file_df['cliente_age'].isnull().sum())

        #   28) Time as a customer

        year_today = date.today().year
        file_df['cliente_fechaini_zurich'] = (file_df['cliente_fechaini_zurich'].astype(float) // 10000)
        file_df['audit_fecha_cierre2'] = (file_df['audit_fecha_cierre'] // 10000).astype(int)
        file_df['audit_fecha_cierre_corta'] = (file_df['audit_fecha_cierre'] // 100).astype(int)
        file_df['cliente_antiguedad'] = file_df.apply(lambda x: Home.time_as_dates(x['cliente_fechaini_zurich'], x['audit_fecha_cierre2']), axis=1)

        print('NULL antiguedad del cliente', file_df['cliente_antiguedad'].isnull().sum())

        #   29) Age of the home (Spanish: Antiguedad de la vivienda)
        file_df['Hogar_antiguedad'] = file_df.apply(lambda x: Home.time_as_dates(x['hogar_anio_construccion'], x['audit_fecha_cierre2']), axis=1)

        #   29) Active customer from the beginning? 1 if it was active all the time, else 0

        file_df['d_cliente_active'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['cliente_incliente'] == True, 'd_cliente_active'] = 1

        print('NULL Clientes activos', file_df['cliente_incliente'].isnull().sum())

        #   30) Time of the policy

        file_df['poliza_fecha_inicio2'] = (file_df['poliza_fecha_inicio'] // 10000).astype(int)
        file_df['poliza_year'] = file_df.apply(lambda x: Home.time_as_dates(x['poliza_fecha_inicio2'], x['audit_fecha_cierre2']), axis=1)
        file_df = file_df.drop('audit_fecha_cierre2', 1)


        print('NULL Tiempo de la poliza', file_df['poliza_year'].isnull().sum())

        #   31) Indice de abandono Hogar & Total

        file_df.loc[file_df['poliza_mediador_numpol_vigor_hogar'].astype(int) > 0, 'ind_aband_Hogar'] = file_df[
                                                                                                            'poliza_mediador_numpol_anuladas_hogar'].astype(float) / file_df['poliza_mediador_numpol_vigor_hogar'].astype(float)
        file_df.loc[file_df['poliza_mediador_numpol_vigor_hogar'] <= 0, 'ind_aband_Hogar'] = np.NaN

        file_df.loc[file_df['poliza_mediador_numpol_vigor'].astype(float) > 0, 'ind_aband_total'] = file_df[
                                                                                                    'poliza_mediador_numpol_anuladas'].astype(float) / file_df['poliza_mediador_numpol_vigor'].astype(float)
        file_df.loc[file_df['poliza_mediador_numpol_vigor'].astype(float) <= 0, 'ind_aband_total'] = np.NaN

        #   32) Segmento de los mediadores
        segmento = ['ESTRATEGICOS', 'ACTIVOS', 'POTENCIAL', 'CENTRALIZADOS']
        file_df.loc[file_df['mediador_segmento_desc'].isin(segmento), 'mediador_segmento'] = file_df['mediador_segmento_desc']
        file_df.loc[~file_df['mediador_segmento_desc'].isin(segmento), 'mediador_segmento'] = 'OTROS'
        #print('Los Segmentos Mediadores', file_df.mediador_segmento.value_counts())

        #dummy_hogar = pd.get_dummies(file_df['mediador_segmento'], prefix='d_mediador_segmento')
        #file_df = pd.concat([file_df, dummy_hogar], axis=1)
        #del dummy_hogar
        file_df = file_df.drop('mediador_segmento_desc', 1)

        #   33) Mediador tipo de comision

        #   There is no null values in this variable, but we can control if this variable gets null values in the future

        file_df['mediador_tipo_comision_cod'] = file_df['mediador_tipo_comision_cod'].fillna('not_found')
        #dummy_hogar = pd.get_dummies(file_df['mediador_tipo_comision_cod'], prefix='d_mediador_comision')
        #file_df = pd.concat([file_df, dummy_hogar], axis=1)
        #del dummy_hogar
        #file_df = file_df.drop('mediador_tipo_comision_cod', 1)

        #   34) Mediation or Partners
        intermediario = ['6300', '6250', '6200']
        file_df.loc[file_df['poliza_estructura_codigo'].isin(intermediario), 'poliza_canal'] = 'mediation'
        file_df.loc[~file_df['poliza_estructura_codigo'].isin(intermediario), 'poliza_canal'] = 'partners'
        #print('Intermediario ', file_df.poliza_canal.value_counts())

        #   35) Identificar si tiene el mismo CP domicilio Vs y el objeto (1 si son iguales)
        file_df['d_same_cp'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['hogar_cp'] == file_df['cliente_codigo_postal'], 'd_same_cp'] = 1

        #   36) Clase de persona (fisica or Juridica)
        file_df['d_clase_persona'] = pd.Series(0, index=file_df.index)
        file_df.loc[file_df['cliente_clase_persona_codigo'] == 'F', 'd_clase_persona'] = 1
        #print('Clase persona \n', file_df.d_clase_persona.value_counts())
        file_df = file_df.drop('cliente_clase_persona_codigo', 1)

        #   37) Clase del Mediador
        file_df['mediacion_tipo'] = 'Otros'
        file_df.loc[((file_df['mediador_clase'] == 'Agente exclusivo') | (file_df['mediador_clase'] == 'Agente vinculado') | (file_df['mediador_clase'] == 'Agente afecto')), 'mediacion_tipo'] = 'Agentes'
        file_df.loc[((file_df['mediador_clase'] == 'Correduría con núm. DGS') | (file_df['mediador_clase'] == 'Corredor') | (file_df['mediador_clase'] == 'Corredor o correduría en trámite con la DGS')), 'mediacion_tipo'] = 'Corredores'
        file_df = file_df.drop('mediador_clase', 1)

        #   38) Cliente CCAA normalizarlo, quitarle acento y cambiar la Ñ por la N

        file_df[['cliente_ccaa']] = file_df['cliente_ccaa'].str.upper()
        file_df['cliente_ccaa'] = file_df['cliente_ccaa'].replace('Ñ', 'N')
        file_df['cliente_ccaa'] = file_df['cliente_ccaa'].map(str)
        file_df['cliente_ccaa'] = file_df['cliente_ccaa'].apply(Home.remove_accents)

        file_df[['hogar_ccaa']] = file_df['hogar_ccaa'].str.upper()
        file_df['hogar_ccaa'] = file_df['hogar_ccaa'].replace('Ñ', 'N')
        file_df['hogar_ccaa'] = file_df['hogar_ccaa'].map(str)
        file_df['hogar_ccaa'] = file_df['hogar_ccaa'].apply(Home.remove_accents)

        file_df[['cliente_provincia']] = file_df['cliente_provincia'].str.upper()
        file_df['cliente_provincia'] = file_df['cliente_provincia'].replace('Ñ', 'N')
        file_df['cliente_provincia'] = file_df['cliente_provincia'].replace(',', '_')
        file_df['cliente_provincia'] = file_df['cliente_provincia'].replace('/', '_')
        file_df['cliente_provincia'] = file_df['cliente_provincia'].map(str)
        file_df['cliente_provincia'] = file_df['cliente_provincia'].apply(Home.remove_accents)


        # DELETE VARIABLES: We delete not necessary variables.
        '''
        del_variables2 = ['hogar_ubicacion_cod','hogar_caracter_cod',
         'hogar_uso_cod','garantia_continente_tipo','garantia_continente_indicador_danios_esteticos','garantia_robo_joyas_capital_base',
         'garantia_robo_objetos_capital_base','garantia_indicador_rc','garantia_indicador_de','garantia_indicador_da',
         'garantia_indicador_cristales','garantia_indicador_defjur','garantia_indicador_robo', 'cliente_tcnacper','cliente_tcnacres',
         'd_cliente_nacionality_spain','d_cliente_residence_spain',
         'd_garantia_indicador_rc', 'd_garantia_indicador_de', 'd_garantia_indicador_da', 'd_garantia_indicador_cristales',
          'd_garantia_indicador_defjur', 'd_garantia_indicador_robo', 'cliente_poliza_hogar_duracion_maxima', 'cliente_fenaccon',
          'cliente_fenaccon2', 'cliente_fechaini_zurich', 'cliente_incliente', 'poliza_fecha_inicio', 'poliza_fecha_inicio2',
          'mediador_segmento_desc','poliza_estructura_desc', 'hogar_poblacion']

          '''


        #   Nota: Borro la nacionalidad y la residencia por recomendacion de Monica, por defecto siempre colocan Espanol

        '''
        # Análisis de outliers OUTLIERS: Get the outliers using MAD from the next variables
        outliers = ['hogar_capital_continente', 'hogar_capital_contenido', 'hogar_m2', 'hogar_anio_construccion']

        for i in outliers:
            Outliers.outliers_mad(file_df, i,just_count_zero=False)  # Primera pasada para eliminar errores humanos por ejemplo ano construccion 0
            outlier_name = str(i) + '_mad_outlier'
            file_df.loc[file_df[outlier_name] == 1, i] = np.NaN  # coloca nulo a los posibles errores humanos
            del file_df[outlier_name]  # OJO se construye una columna que identifica el outlier pero lo borra porque no me interesa identificar errores humnaos
            Outliers.outliers_mad(file_df, i,just_count_zero=False)  # aqui se crea la columna que identifica el outlier en esa función "outliers_mad"
            file_df.loc[file_df[i] == 0, i] = np.NaN
            '''

        #   Filtros

        #file_df2 = file_df.drop(del_variables2, 1)
        file_df2 = file_df

        print('Numero de variables hasta ahora', file_df2.shape)

        return file_df2

    def naive_age(date1, date2):
        naive_yrs = date2 - date1
        if date1 == 0 or naive_yrs < 18 or naive_yrs > 120:
            return 0
        else:
            return naive_yrs

    def time_as_dates(date1, date2):
        if date1 == np.nan:
            return date1
        else:
            time_date = date2 - date1
            if time_date < 0:
                return 0
            else:
                return time_date

    def remove_accents(a):
        return unidecode.unidecode(a)

    def filtering(file_df, file_definitive):
        #   Filtering not personal line
        estructura = ['6700', '9999']
        file_df = file_df.loc[~file_df['poliza_estructura_codigo'].isin(estructura)]

        #   Deleting DB
        '''00222 - Personal Accidents
            00555 - Household
            00777 - Motor'''

        product = ['00555']
        file_df = file_df.loc[~file_df['poliza_producto_tecnico'].isin(product)]

        #   Filtering BS as Alex said in an email

        file_df['poliza_producto_tecnico2'] = file_df['poliza_producto_tecnico'].map(str) + '_' + file_df['poliza_producto_comercial'].map(str)
        file_df['poliza_producto_tecnico2'] = file_df['poliza_producto_tecnico2'].astype(object)

        #   this product we have to delete it with the combination of codigo_padre

        producto_tecnico2 = [
            '00215_00001',
            '00215_00003',
            '00481_00001',
            '00483_00001',
            '00504_00001',
            '00515_00002',
            '00515_00006',
            '00525_00006',
            '00589_00001',
            '00591_00001',
            '00632_00001',
            '00663_00001',
            '00671_00001',
            '00674_00001',
            '00677_00001',
            '00715_00001',
            '00715_00001',
            '00715_00002',
            '00715_00002',
            '00842_00001',
            '00845_00001',
            '00850_00003',
            '00886_00001']

        padre = ['9099999084', '9099999104', '9099999338', '9099999503']

        file_df['delete_bs'] = 0
        file_df.loc[(file_df['poliza_producto_tecnico2'].isin(producto_tecnico2)) & (file_df['poliza_negocio_padre_codigo'].isin(padre)), 'delete_bs'] = 1

        file_df = file_df.loc[file_df['delete_bs'] == 0]
        file_df = file_df.drop('delete_bs', 1)

        file_df['delete_partners'] = 0
        file_df.loc[(file_df['poliza_negocio_padre_codigo'].isnull()) & (file_df['poliza_canal'] == 'partners'), 'delete_partners'] = 1

        #   Elimino los partners que no tiene Codigo padre
        #print('Deleting  all the partners with do not have any codigo_padre \n', file_df.delete_partners.value_counts())
        file_df = file_df.loc[file_df['delete_partners'] == 0]
        file_df = file_df.drop('delete_partners', 1)

        #   Elimino

        #   Elimino las polizas que no estan en Vigor
        file_df = file_df.loc[(file_df['poliza_estado'] == 'V')]

        #   Eliminar cambio de tomador
        file_df = file_df.loc[(file_df['poliza_cambio_tomador'] == 0)]
        file_df = file_df.drop('poliza_cambio_tomador', 1)

        #   Get the number of home policies: Ver cuantas polizas de hogar despues de los filtros
        file_df = pd.merge(file_df, file_definitive, how='left', on='cliente_codfiliacion')
        file_df['numpol_hogar'] = file_df.groupby('cod_global_unico')['cod_global_unico'].transform('size')

        #   Obtener la antiguedad maxima de la poliza por cliente

        file_df['max_ant_vigor_hogar'] = file_df.groupby('cod_global_unico')['poliza_year'].transform('max')


        #   Eliminar polizas mayores a 10 pólizas de hogares de un solo cod_unico_global
        file_df = file_df.loc[(file_df['numpol_hogar'] <= 10)]

        return file_df

    def get_cartera(Botella):
        """
        Get all the active policies that need to renuew into 3 month after the closing date, i.e.
        (Spanish) Escoger todas las carteras en Vigor que deben renovar 3 meses después de la fecha de cierre

        :return: Only the "cartera" corresponding to that closing date (fecha de cierre)
        """
        #print("Distribuccion de la cartera \n", Botella.poliza_fecha_cartera2.value_counts().sort_index())

        plus_month_period = 3   # 3 meses para seleccionar la cartera a partir de la fecha de cierre, se toma entonces la variable audit_fecha_cierre
        Botella['audit_fecha_cierre'] = Botella['audit_fecha_cierre'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d'))
        # Sumar 3 meses es decir audit_fecha_cierre_cartera es fecha futura
        Botella['audit_fecha_cierre_cartera'] = Botella.audit_fecha_cierre + pd.DateOffset(months=plus_month_period)
        Botella['audit_fecha_cierre_cartera'] = Botella['audit_fecha_cierre_cartera'].apply(lambda x: x.strftime('%Y%m'))

        Botella['poliza_fecha_cartera2'] = Botella['poliza_fecha_cartera2'].map(int)
        Botella['audit_fecha_cierre_cartera'] = Botella['audit_fecha_cierre_cartera'].map(int)

        cartera = Botella[Botella['poliza_fecha_cartera2'] == Botella['audit_fecha_cierre_cartera']]
        cartera = cartera[cartera.poliza_estado == "V"]

        return cartera

    def get_target(cartera):
        """
        Esto está MAL!!! porque hay errores en los snapshot, asi que se va a construir otra funcion que "arregle" el
        problema de las anulaciones, si se llega a solucionar en el futuro las causisticas que están documentadas
        en el word "Z:\ADVANCED ANALYTICS\2017_Proyecto Abandono Auto\5 Funcional\Validaciones Target values"
         entonces SI se puede usar esta funcion, por ahora NO se va a usar.

        Add the target for each portfolio, for this we will see 4 moths after expire date.
        (Spanish) Colocar la variable a predecir "target" a cada cartera, miraremos 4 meses después de la fecha cartera

        :return: the "cartera" with the target value
        """
        #   Here we have to choose the target value, for this we should see the bottle of 4 months later.
        cartera_date = cartera.iloc[0]['audit_fecha_cierre_cartera']    #Get the exact date that I have to look into the right bottle
        #   cartear_date (Spanish) extraigo la fecha donde debo buscar si la poliza churn o no. Aqui tengo el anio&mes
        plus_month_period = 4
        cartera['month'] = pd.to_datetime(cartera['audit_fecha_cierre_cartera'], format='%Y%m')
        # cartera1['month2'] = cartera1['audit_fecha_cierre_cartera'].apply(lambda x: x.strftime('%Y%m'))
        cartera['fecha_target'] = cartera.month + pd.DateOffset(months=plus_month_period)
        cartera['fecha_target'] = cartera['fecha_target'].apply(lambda x: x.strftime('%Y%m'))
        cartera['fecha_target'] = cartera['fecha_target'].map(int)
        #print(cartera.month.value_counts())
        #print(cartera.fecha_target.value_counts())
        target_cartera = cartera.iloc[0]['month']
        target_date = cartera.iloc[0]['fecha_target']
        target_date = str(target_date)
        cartera = cartera.drop(['month'], 1)

        files = set([f for f in os.listdir(PATH_VAR.path_out)])
        Hogar_files = [i for i in files if target_date in i]

        if len(Hogar_files) == 1:
            for i in Hogar_files:
                print(i)
                bottle = l.load_file_HOGAR_matrix(PATH_VAR.path_out, i)
                print('Botella Hogar \n', i, '\n', bottle.shape)
                target = bottle[
                    ['poliza_id', 'poliza_estado', 'poliza_motivo_cancelacion_desc', 'poliza_motivo_cancelacion_codigo',
                     'poliza_fecha_emision_anulacion', 'poliza_fecha_cambio_estado', 'poliza_fecha_vencimiento_nat']]
                target = target.rename(columns={'poliza_estado': 'poliza_estado_final'})
                target['poliza_fecha_cartera_target'] = target['poliza_fecha_vencimiento_nat'] // 100
                target = target.drop('poliza_fecha_vencimiento_nat', 1)
                target = pd.merge(cartera, target, how='left', on='poliza_id')

                #   En este momento estoy construyendo la variable CHURN
                target['poliza_churn'] = pd.Series(0, index=target.index)
                target.loc[(target['poliza_estado_final'].isnull()), 'poliza_estado_final'] = 'A'
                target.loc[target['poliza_estado_final'] == 'A', 'poliza_churn'] = 1
                print(target.poliza_churn.value_counts())
        else:
            print('ERROR must be only one bottle')

        '''Grupo1: Agravamiento del riesgo, Anulación por llegada a término, Insatisfacción cliente, Petición del cliente y prima elevada (
            Grupo 2: Desaparición del riesgo
            Grupo 3: Falta de pago
            Grupo 4: Petición Mediador'''
        group1 = ['218', '680', '702', '115', '707']
        group2 = ['203']
        group3 = ['101']
        group4 = ['703']
        saneamiento = ['165', '114', '705', '706']

        #   quitar saneamiento
        #target = target[target['poliza_motivo_cancelacion_codigo'].isin(saneamiento)]

        target['poliza_motivo_cancelacion2'] = None
        target.loc[target['poliza_motivo_cancelacion_codigo'].isin(saneamiento), 'poliza_motivo_cancelacion2'] = '99' # Debo quitar saneamiento
        target.loc[target['poliza_motivo_cancelacion_codigo'].isin(group1), 'poliza_motivo_cancelacion2'] = '1'  # Grupo1
        target.loc[target['poliza_motivo_cancelacion_codigo'].isin(group2), 'poliza_motivo_cancelacion2'] = '2'  # Grupo2
        target.loc[target['poliza_motivo_cancelacion_codigo'].isin(group3), 'poliza_motivo_cancelacion2'] = '3'  # Grupo3
        target.loc[target['poliza_motivo_cancelacion_codigo'].isin(group4), 'poliza_motivo_cancelacion2'] = '4'  # Grupo4

        target = target.loc[~target['poliza_motivo_cancelacion_codigo'].isin('99')] #   Aqui quitamos saneamiento y sin efecto

        return target

    def get_target_new(cartera):
        """
        Este es el que se va a usar por ahora, aqui se trata de corregir los errores que hay en cuanto a los valores de
        los estados "A" o "V".

        Add the target for each portfolio
        (Spanish) Colocar la variable a predecir "target" a cada cartera, miraremos 4 meses después de la fecha cartera

        En este caso seguiremos la siguiente implementacion (debido a que las variables poliza_estado no están correctas)
        Este sería el seudo-codigo

        B3: Botella en la fecha de la cartera
        B6: Botella en la consolidacion de la cartera (4 meses después) por ejemplo:
        Si el cierre de mes es el 201604 entonces B3: Botella cierre 201607 y B6 : Botella a cierre 201611

        If B3.poliza_estado == V && B6.poliza_estado == A then poliza_churn=1 	# (Caso 4)
        If B3.poliza_estado == V && B6.poliza_estado == V then poliza_churn=0 	# (Caso 5)
        If B3.poliza_estado == A && (B6.poliza_estado == A || is.null (B6.poliza_estado)  then poliza_churn=1 # (Caso 6)
        If B3.poliza_estado == A && (B6.poliza_estado == V)  then poliza_churn=1 # (Caso 1)
        If B3.poliza_estado == V && is.null (B6.poliza_estado)  then poliza_churn=1 # (Caso 7)

        If B3.poliza_estado == R then poliza_churn=0 # (Caso Reemplazo)
        If B3.poliza_estado == V && (B6.poliza_estado == R) then poliza_churn=0 # (Caso Reemplazo)

        :return: the "cartera" with the target value
        """
        #   Here we have to choose the target value, for this we should see the bottle of 4 months later.
        cartera_date = cartera.iloc[0]['audit_fecha_cierre_cartera']  # Get the exact date that I have to look into the right bottle
        cartera_date = str(cartera_date)
        #   cartear_date (Spanish) extraigo la fecha donde debo buscar si la poliza churn o no. Aqui tengo el anio&mes
        plus_month_period = 4
        cartera['month'] = pd.to_datetime(cartera['audit_fecha_cierre_cartera'], format='%Y%m')
        # cartera1['month2'] = cartera1['audit_fecha_cierre_cartera'].apply(lambda x: x.strftime('%Y%m'))
        cartera['fecha_target'] = cartera.month + pd.DateOffset(months=plus_month_period)
        cartera['fecha_target'] = cartera['fecha_target'].apply(lambda x: x.strftime('%Y%m'))
        cartera['fecha_target'] = cartera['fecha_target'].map(int)
        #print(cartera.month.value_counts())
        #print(cartera.fecha_target.value_counts())

        target_date = cartera.iloc[0]['fecha_target']
        target_date = str(target_date)
        cartera = cartera.drop(['month'], 1)

        files = set([f for f in os.listdir(PATH_VAR.path_out)])
        hogar_files3 = [i for i in files if cartera_date in i]
        hogar_files6 = [i for i in files if target_date in i]
        hogar_files = list(set().union(hogar_files3, hogar_files6))
        hogar_files.sort()

        if len(hogar_files) == 2:
            i = hogar_files[0]
            #   Introduciendo los datos del estado en la foto de la cartera le puedomos llamar botella3
            print(i)
            bottle = l.load_file_HOGAR_matrix(PATH_VAR.path_out, i)
            print('Botella Hogar \n', i, '\n', bottle.shape)
            b3 = bottle[
                ['poliza_id', 'poliza_estado', 'poliza_motivo_cancelacion_desc', 'poliza_motivo_cancelacion_codigo',
                 'poliza_fecha_emision_anulacion', 'poliza_fecha_cambio_estado']]
            b3 = b3.rename(columns={'poliza_estado': 'poliza_estado_3',
                                'poliza_motivo_cancelacion_desc':'poliza_motivo_cancelacion_desc_3',
                                'poliza_motivo_cancelacion_codigo' : 'poliza_motivo_cancelacion_codigo_3',
                                'poliza_fecha_emision_anulacion' : 'poliza_fecha_emision_anulacion_3',
                                'poliza_fecha_cambio_estado': 'poliza_fecha_cambio_estado_3'})
            #b3['poliza_fecha_cartera_target'] = target['poliza_fecha_vencimiento_nat'] // 100
            #b3 = b3.drop('poliza_fecha_vencimiento_nat', 1)
            b3 = pd.merge(cartera, b3, how='left', on='poliza_id')

            #print("Antes de saneamiento ", b3.shape)
            b3 = Home.delete_saneamiento(b3)
            #print("Despues de saneamiento ", b3.shape)

            # AHORA para el target
            i = hogar_files[1]
            print(i)
            b6 = l.load_file_HOGAR_matrix(PATH_VAR.path_out, i)
            b6 = b6[
                ['poliza_id', 'poliza_estado', 'poliza_motivo_cancelacion_desc', 'poliza_motivo_cancelacion_codigo',
                 'poliza_fecha_emision_anulacion', 'poliza_fecha_cambio_estado']]
            b6 = b6.rename(columns={'poliza_estado': 'poliza_estado_6',
                                            'poliza_motivo_cancelacion_desc': 'poliza_motivo_cancelacion_desc_6',
                                            'poliza_motivo_cancelacion_codigo': 'poliza_motivo_cancelacion_codigo_6',
                                            'poliza_fecha_emision_anulacion': 'poliza_fecha_emision_anulacion_6',
                                            'poliza_fecha_cambio_estado': 'poliza_fecha_cambio_estado_6'})
            # target['poliza_fecha_cartera_target'] = target['poliza_fecha_vencimiento_nat'] // 100
            # target = target.drop('poliza_fecha_vencimiento_nat', 1)
            #print("target antes ", b6.shape)
            target = pd.merge(b3, b6, how='left', on='poliza_id')
            #print("target despues ", target.shape)

            #   En este momento estoy construyendo la variable CHURN

            target['p_churn_casos'] = 'XX'
            target.loc[(target['poliza_estado_3'] == 'V') & (target['poliza_estado_6'] == 'A'), 'p_churn_casos'] = "caso4"
            target.loc[(target['poliza_estado_3'] == 'V') & (target['poliza_estado_6'] == 'V'), 'p_churn_casos'] = "caso5"
            target.loc[(target['poliza_estado_3'] == 'A') & ((target['poliza_estado_6'] == 'A') | (target['poliza_estado_6'].isnull())), 'p_churn_casos'] = "caso6"
            target.loc[(target['poliza_estado_3'] == 'A') & (target['poliza_estado_6'] == 'V'), 'p_churn_casos'] = "caso1"
            target.loc[(target['poliza_estado_3'] == 'V') & (target['poliza_estado_6'].isnull()), 'p_churn_casos'] = "caso7"
            target.loc[(target['poliza_estado_3'] == 'R'), 'p_churn_casos'] = "casoR"
            target.loc[(target['poliza_estado_3'] == 'V') & (target['poliza_estado_6'] == 'R'), 'p_churn_casos'] = "casoR"

            casos_churn = ['caso4', 'caso6', 'caso1', 'caso7']
            target['poliza_churn'] = pd.Series(0, index=target.index)
            target.loc[target['p_churn_casos'].isin(casos_churn), 'poliza_churn'] = 1  # Creacion de la variable CHURN
            #print(target.poliza_churn.value_counts())
            #print(target['poliza_churn'].isnull().sum())
            delete_variable = ['p_churn_casos','poliza_estado_3', 'poliza_motivo_cancelacion_desc_6', 'poliza_motivo_cancelacion_codigo_6', 'poliza_fecha_emision_anulacion_6', 'poliza_fecha_cambio_estado_6','poliza_estado_6']
            target = target.drop(delete_variable, 1)
            return target
        else:
            print('ERROR must be two bottles')
            return cartera

    def delete_saneamiento(target):

        '''
        In this case, we are not interesting having policies that the insurance already churn because of loss (lot of claims)
        (Spanish) en este caso no estamos interesados en tener polizas que la aseguradora dio de baja debido a las perdidas (muchos siniestros)
        :arg la cartera que previamiente se le calculo la variable churn

        :return cartera sin saneamientos
                    Grupo1: Agravamiento del riesgo, Anulación por llegada a término, Insatisfacción cliente, Petición del cliente y prima elevada (
                    Grupo 2: Desaparición del riesgo
                    Grupo 3: Falta de pago
                    Grupo 4: Petición Mediador

                    codigo:
                    218: Agravación del riesgo
                    680: Anulación por llegada a término
                    702: Insatisfacción cliente
                    115: Petición del cliente
                    707: Prima elevada
                    165: Mal rendimiento
                    114: Reemplazo poliza
                    705: Saneamiento intermediario/negocio
                    706: Sin efecto

                    '''
        group1 = ['218', '680', '702', '115', '707']
        group2 = ['203']
        group3 = ['101']
        group4 = ['703']
        saneamiento = ['165', '114', '705', '706'] # En realidad es limpieza de saneamiento y sin efecto  reportado

        #   quitar saneamiento
        # target = target[target['poliza_motivo_cancelacion_codigo'].isin(saneamiento)]

        target['poliza_motivo_cancelacion2'] = None
        target.loc[target['poliza_motivo_cancelacion_codigo_3'].isin(saneamiento), 'poliza_motivo_cancelacion2'] = '99'  # Debo quitar saneamiento
        target.loc[target['poliza_motivo_cancelacion_codigo_3'].isin(group1), 'poliza_motivo_cancelacion2'] = '1'  # Grupo1
        target.loc[target['poliza_motivo_cancelacion_codigo_3'].isin(group2), 'poliza_motivo_cancelacion2'] = '2'  # Grupo2
        target.loc[target['poliza_motivo_cancelacion_codigo_3'].isin(group3), 'poliza_motivo_cancelacion2'] = '3'  # Grupo3
        target.loc[target['poliza_motivo_cancelacion_codigo_3'].isin(group4), 'poliza_motivo_cancelacion2'] = '4'  # Grupo4

        #target = target.loc[~target['poliza_motivo_cancelacion_codigo_3'].isin('99')]  # Aqui quitamos saneamiento y sin efecto
        target = target[target["poliza_motivo_cancelacion2"].isin(["99"]) == False]

        return target

    def delete_falsos_reemplazos(cartera):
        set_anul = cartera[cartera['poliza_churn'] == 1]

        return set_anul

    def delete_sin_efecto(cartera):
        cartera['poliza_fecha_inicio2'] = cartera['poliza_fecha_inicio'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d'))
        cartera['poliza_fecha_emision_anulacion2'] = cartera['poliza_fecha_emision_anulacion'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d'))
        cartera['poliza_days_alive'] = (cartera['poliza_fecha_emision_anulacion2'] - cartera['poliza_fecha_inicio2']).dt.days

        cartera['delete_sin_efecto'] = pd.Series(0, index=cartera.index)
        cartera.loc[cartera['poliza_days_alive'] <= 30, 'delete_sin_efecto'] = 1

        print("Numero de polizas sin efecto: ", print(cartera.delete_sin_efecto.value_counts()))
        cartera = cartera[cartera['delete_sin_efecto'] == 0]

        cartera = cartera.drop('delete_sin_efecto', 1)

        return cartera

    def del_sin_effecto(df_4):
        # this function delete inactive sin effecto policies in all products except hogar
        # RETURN: the same df but cleaning (deleting) those sin efecto policies
        # put all inactive policies in df_poliza_inactive
        df_poliza_inactive = df_4[(df_4['poliza_estado'].isin(['A']))]

        #  find out indirect sin effectos where different start date and end date of policy is within 60 days

        # calculate the difference in start date and end date of the policy
        df_poliza_inactive['poliza_fecha_inicio'] = pd.to_datetime(df_poliza_inactive['poliza_fecha_inicio'],
                                                                   format='%Y%m%d')

        df_poliza_inactive.loc[df_poliza_inactive['poliza_fecha_cambio_estado'] == 0, 'poliza_fecha_cambio_estado'] = \
            df_poliza_inactive['poliza_fecha_emision']

        df_poliza_inactive['poliza_fecha_cambio_estado'] = pd.to_datetime(
            df_poliza_inactive['poliza_fecha_cambio_estado'], format='%Y%m%d')
        df_poliza_inactive['poliza_fecha_emision'] = pd.to_datetime(df_poliza_inactive['poliza_fecha_emision'],
                                                                    format='%Y%m%d')

        df_poliza_inactive['first_difference'] = df_poliza_inactive['poliza_fecha_cambio_estado'].sub(
            df_poliza_inactive['poliza_fecha_inicio'], axis=0)
        df_poliza_inactive['cambio_difference'] = df_poliza_inactive['first_difference'] / np.timedelta64(1, 'D')

        df_poliza_inactive['second_difference'] = df_poliza_inactive['poliza_fecha_emision'].sub(
            df_poliza_inactive['poliza_fecha_inicio'], axis=0)
        df_poliza_inactive['emision_difference'] = df_poliza_inactive['second_difference'] / np.timedelta64(1, 'D')

        df_sin_efecto_definite = df_poliza_inactive[
            (df_poliza_inactive['cambio_difference'] <= 60) & (df_poliza_inactive['emision_difference'] <= 60) & (
            df_poliza_inactive['poliza_agrup_prod_cod'] == 'HOGAR')]  # 16515
        df_sin_efecto_not_definite = df_poliza_inactive[
            (df_poliza_inactive['poliza_version'] <= 3) & (df_poliza_inactive['poliza_agrup_prod_cod'] == 'HOGAR') & (
            ((df_poliza_inactive['cambio_difference'] <= 60) & (df_poliza_inactive['emision_difference'] > 60)) | (
            (df_poliza_inactive['cambio_difference'] > 60) & (df_poliza_inactive['emision_difference'] <= 60)))]

        # delete definite sin effectos
        df_5_0 = df_4[~df_4.poliza_id.isin(df_sin_efecto_definite.poliza_id)]
        # delete not definite sin effectos
        df_5_1 = df_5_0[~df_5_0.poliza_id.isin(df_sin_efecto_not_definite.poliza_id)]
        return df_5_1

    def claims_borrar(self):

        files = set([f for f in os.listdir(PATH_VAR.path_out)])
        claim_file = [i for i in files if 'claim' in i]
        i = claim_file[0]
        #   Introduciendo los datos del estado en la foto de la cartera le puedomos llamar botella3
        print(i)
        bottle = l.load_file_HOGAR_matrix(PATH_VAR.path_out, i)
        return bottle

    def get_claims(Bclaims, cartera):
        """
        :param cartera: las polizas perteneciente a la cartera
        :return: Returna la cartera con las variablesDE siniestros
        """

        fecha_target = cartera.iloc[0]['poliza_fecha_cartera2']
        pd.to_numeric(fecha_target, errors='coerce')
        # fecha_target = str(fecha_target)

        fecha_cierre = cartera.iloc[0]['audit_fecha_cierre_corta']
        pd.to_numeric(fecha_cierre, errors='coerce')
        # fecha_cierre = str(fecha_cierre)

        fecha_cierre_ago = fecha_cierre - 100

        Bclaims['siniestro_fecha_apertura2'] = (Bclaims['siniestro_fecha_apertura'] // 100).astype(int)

        fecha_cierre_year = fecha_cierre // 100
        Bclaims_12 = Bclaims[((Bclaims['siniestro_fecha_apertura2'] <= fecha_cierre) &
                              (fecha_cierre_ago <= Bclaims['siniestro_fecha_apertura2']))]

        Bclaims_12['siniestro_fecha_apertura3'] = pd.to_datetime(Bclaims_12['siniestro_fecha_apertura'],
                                                                 format='%Y%m%d')

        Bclaims_12.loc[Bclaims_12['siniestro_fecha_fin'] == 0, 'siniestro_fecha_fin'] = np.NaN
        Bclaims_12['siniestro_fecha_fin3'] = pd.to_datetime(Bclaims_12['siniestro_fecha_fin'], format='%Y%m%d')

        Bclaims_12['siniestro_diasresol'] = Bclaims_12['siniestro_fecha_fin3'].sub(
            Bclaims_12['siniestro_fecha_apertura3'], axis=0)

        Bclaims_12['siniestro_diasresol'] = Bclaims_12['siniestro_diasresol'] / np.timedelta64(1, 'D')

        poliza_num_siniestros = Bclaims_12.groupby(['poliza_id']).size().reset_index(name='poliza_num_siniestros')

        poliza_num_siniestros = Bclaims_12.groupby(['poliza_id']).size().reset_index(name='poliza_num_siniestros')
        Bclaims_12['poliza_siniestro_diasresol_max'] = Bclaims_12.groupby('poliza_id')['siniestro_diasresol'].transform(
            'max')

        t_max_dia_resol = Bclaims_12.drop_duplicates(['poliza_id'], keep='first')
        t_max_dia_resol = t_max_dia_resol[['poliza_id', 'poliza_siniestro_diasresol_max']]
        # print(t_max_dia_resol.head())

        Bclaims_12['siniestro_fecha_apert_reciente'] = Bclaims_12.groupby('poliza_id')[
            'siniestro_fecha_apertura'].transform('max')
        t_siniestro_fecha_apert_reciente = Bclaims_12.drop_duplicates(['poliza_id'], keep='first')
        t_siniestro_fecha_apert_reciente = t_siniestro_fecha_apert_reciente[
            ['poliza_id', 'siniestro_fecha_apert_reciente']]

        siniestros_terminados = Bclaims_12[Bclaims_12['siniestro_estado_siniestro'] == 'T']
        siniestros_pendiente = Bclaims_12[Bclaims_12['siniestro_estado_siniestro'] == 'P']

        poliza_num_siniestros_terminado = siniestros_terminados.groupby(['poliza_id']).size().reset_index(
            name='poliza_num_siniestros_terminados')
        poliza_num_siniestros_pendiente = siniestros_pendiente.groupby(['poliza_id']).size().reset_index(
            name='poliza_num_siniestros_pendiente')



        siniestros_terminados['siniestro_fecha_apert_reciente_t'] = siniestros_terminados.groupby('poliza_id')[
            'siniestro_fecha_apertura'].transform('max')
        t_siniestros_terminados = siniestros_terminados.drop_duplicates(['poliza_id'], keep='first')
        t_siniestros_terminados = t_siniestros_terminados[['poliza_id', 'siniestro_fecha_apert_reciente_t']]


        siniestros_pendiente['siniestro_fecha_apert_reciente_p'] = siniestros_pendiente.groupby('poliza_id')[
            'siniestro_fecha_apertura'].transform('max')
        t_siniestros_pendiente = siniestros_pendiente.drop_duplicates(['poliza_id'], keep='first')
        t_siniestros_pendiente = t_siniestros_pendiente[['poliza_id', 'siniestro_fecha_apert_reciente_p']]

        cartera_merge = pd.merge(cartera, poliza_num_siniestros, how='left', on='poliza_id')
        cartera_merge = pd.merge(cartera_merge, poliza_num_siniestros_terminado, how='left', on='poliza_id')
        cartera_merge = pd.merge(cartera_merge, poliza_num_siniestros_pendiente, how='left', on='poliza_id')
        cartera_merge = pd.merge(cartera_merge, t_siniestro_fecha_apert_reciente, how='left', on='poliza_id')

        cartera_merge = pd.merge(cartera_merge, t_max_dia_resol, how='left', on='poliza_id')
        cartera_merge = pd.merge(cartera_merge, t_siniestros_terminados, how='left', on='poliza_id')
        cartera_merge = pd.merge(cartera_merge, t_siniestros_pendiente, how='left', on='poliza_id')

        cartera_merge.loc[cartera_merge['poliza_num_siniestros'].isnull(), 'poliza_num_siniestros'] = 0
        cartera_merge.loc[
            cartera_merge['poliza_num_siniestros_terminados'].isnull(), 'poliza_num_siniestros_terminados'] = 0
        cartera_merge.loc[
            cartera_merge['poliza_num_siniestros_pendiente'].isnull(), 'poliza_num_siniestros_pendiente'] = 0

        cartera_merge['d_siniestro'] = pd.Series(0, index=cartera_merge.index)
        cartera_merge.loc[cartera_merge['poliza_num_siniestros'] > 0, 'd_siniestro'] = 1

        cartera_merge['d_siniestro_pendiente'] = pd.Series(0, index=cartera_merge.index)
        cartera_merge.loc[cartera_merge['poliza_num_siniestros_pendiente'] > 0, 'd_siniestro_pendiente'] = 1

        cartera_merge['siniestro_fecha_apert_reciente_2'] = pd.to_datetime(
            cartera_merge['siniestro_fecha_apert_reciente'], format='%Y%m%d')

        cartera_merge['audit_fecha_cierre'] = pd.to_datetime(cartera_merge['audit_fecha_cierre'])

        cartera_merge['siniestro_fecha_apert_reciente_poliza_m'] = cartera_merge['audit_fecha_cierre'].sub(
            cartera_merge['siniestro_fecha_apert_reciente_2'], axis=0)

        cartera_merge['siniestro_fecha_apert_reciente_poliza_m'] = cartera_merge[
                                                                       'siniestro_fecha_apert_reciente_poliza_m'] / np.timedelta64(
            1, 'D')

        cartera_merge['siniestro_fecha_apert_reciente_poliza_m'] = (cartera_merge[
                                                                        'siniestro_fecha_apert_reciente_poliza_m'] / 365) * 12

        '''

        cartera_merge['siniestro_fecha_apert_reciente_p2'] = pd.to_datetime(
            cartera_merge['siniestro_fecha_apert_reciente_p'], format='%Y%m%d')

        # cartera_merge['audit_fecha_cierre'] = pd.to_datetime(cartera_merge['audit_fecha_cierre'])

        cartera_merge['p_fch_apert_claim_rcnt_pend_m'] = cartera_merge['audit_fecha_cierre'].sub(
            cartera_merge['siniestro_fecha_apert_reciente_p2'], axis=0)

        cartera_merge['p_fch_apert_claim_rcnt_pend_m'] = cartera_merge[
                                                             'p_fch_apert_claim_rcnt_pend_m'] / np.timedelta64(1, 'D')

        cartera_merge['p_fch_apert_claim_rcnt_pend_m'] = (cartera_merge['p_fch_apert_claim_rcnt_pend_m'] / 365) * 12
        '''

        ##################
        ####Customer view para contar los siniestros
        #################
        siniestros_auto = Bclaims_12[Bclaims_12['poliza_agrup_prod_desc'] == 'AUTOS']
        siniestros_hogar = Bclaims_12[Bclaims_12['poliza_agrup_prod_desc'] == 'HOGAR']
        siniestros_otros = Bclaims_12[~(Bclaims_12['poliza_agrup_prod_desc'].isin(['AUTOS', 'HOGAR']))]

        # Aqui queremos ver los siniestros que tuvieron los clientes en otros productos
        auto_num_siniestros = siniestros_auto.groupby(['cod_global_unico']).size().reset_index(
            name='auto_num_siniestros')
        hogar_num_siniestros = siniestros_hogar.groupby(['cod_global_unico']).size().reset_index(
            name='hogar_num_siniestros')
        otros_num_siniestros = siniestros_otros.groupby(['cod_global_unico']).size().reset_index(
            name='otros_num_siniestros')



        # Colocar las nuevas variables en la cartera numero de siniestros customer view
        cartera_merge = pd.merge(cartera_merge, auto_num_siniestros, how='left', on='cod_global_unico')
        cartera_merge = pd.merge(cartera_merge, hogar_num_siniestros, how='left', on='cod_global_unico')
        cartera_merge = pd.merge(cartera_merge, otros_num_siniestros, how='left', on='cod_global_unico')

        cartera_merge.loc[cartera_merge['hogar_num_siniestros'].isnull(), 'hogar_num_siniestros'] = 0

        # Ver si tuvo algun siniestro por cliente sin importar el producto
        total_num_siniestros = Bclaims_12.groupby(['cod_global_unico']).size().reset_index(name='total_num_siniestros')
        cartera_merge = pd.merge(cartera_merge, total_num_siniestros, how='left', on='cod_global_unico')
        cartera_merge.loc[(cartera_merge['total_num_siniestros'].isnull()), 'total_num_siniestros'] = cartera_merge[
            'hogar_num_siniestros']

        # Aqui vemos la fecha mas reciente de apertura de otros productos que tiene contratado

        # Para Auto
        siniestros_auto['siniestro_fecha_apert_reciente_auto'] = siniestros_auto.groupby('cod_global_unico')[
            'siniestro_fecha_apertura'].transform('max')
        t_siniestros_auto = siniestros_auto.drop_duplicates(['cod_global_unico'], keep='first')
        t_siniestros_auto = t_siniestros_auto[['cod_global_unico', 'siniestro_fecha_apert_reciente_auto']]
        #print(t_siniestros_auto.head())

        # Para Hogar
        siniestros_hogar['siniestro_fecha_apert_reciente_hogar'] = siniestros_hogar.groupby('cod_global_unico')[
            'siniestro_fecha_apertura'].transform('max')
        t_siniestros_hogar = siniestros_hogar.drop_duplicates(['cod_global_unico'], keep='first')
        t_siniestros_hogar = t_siniestros_hogar[['cod_global_unico', 'siniestro_fecha_apert_reciente_hogar']]
        #print(t_siniestros_hogar.head())

        # Para Otros productos

        siniestros_otros['siniestro_fecha_apert_reciente_otros'] = siniestros_otros.groupby('cod_global_unico')[
            'siniestro_fecha_apertura'].transform('max')
        t_siniestros_otros = siniestros_otros.drop_duplicates(['cod_global_unico'], keep='first')
        t_siniestros_otros = t_siniestros_otros[['cod_global_unico', 'siniestro_fecha_apert_reciente_otros']]
        #print(t_siniestros_otros.head())

        # Colocar las nuevas variables en la cartera fecha de siniestro mas reciente de siniestros customer view

        cartera_merge = pd.merge(cartera_merge, t_siniestros_auto, how='left', on='cod_global_unico')
        cartera_merge = pd.merge(cartera_merge, t_siniestros_hogar, how='left', on='cod_global_unico')
        cartera_merge = pd.merge(cartera_merge, t_siniestros_otros, how='left', on='cod_global_unico')

        # Dias de siniestros abiertos customer view en meses

        # AUTO
        cartera_merge['siniestro_fecha_apert_reciente_auto2'] = pd.to_datetime(
            cartera_merge['siniestro_fecha_apert_reciente_auto'], format='%Y%m%d')

        cartera_merge['audit_fecha_cierre'] = pd.to_datetime(cartera_merge['audit_fecha_cierre'])

        cartera_merge['siniestro_fecha_apert_reciente_auto_m'] = cartera_merge['audit_fecha_cierre'].sub(
            cartera_merge['siniestro_fecha_apert_reciente_auto2'], axis=0)

        cartera_merge['siniestro_fecha_apert_reciente_auto_m'] = cartera_merge[
                                                                     'siniestro_fecha_apert_reciente_auto_m'] / np.timedelta64(
            1, 'D')

        cartera_merge['siniestro_fecha_apert_reciente_auto_m'] = (cartera_merge[
                                                                      'siniestro_fecha_apert_reciente_auto_m'] / 365) * 12

        # HOGAR
        cartera_merge['siniestro_fecha_apert_reciente_hogar2'] = pd.to_datetime(
            cartera_merge['siniestro_fecha_apert_reciente_hogar'], format='%Y%m%d')

        cartera_merge['siniestro_fecha_apert_reciente_hogar_m'] = cartera_merge['audit_fecha_cierre'].sub(
            cartera_merge['siniestro_fecha_apert_reciente_hogar2'], axis=0)

        cartera_merge['siniestro_fecha_apert_reciente_hogar_m'] = cartera_merge[
                                                                      'siniestro_fecha_apert_reciente_hogar_m'] / np.timedelta64(
            1, 'D')

        cartera_merge['siniestro_fecha_apert_reciente_hogar_m'] = (cartera_merge[
                                                                       'siniestro_fecha_apert_reciente_hogar_m'] / 365) * 12

        # OTROS
        cartera_merge['siniestro_fecha_apert_reciente_otros2'] = pd.to_datetime(
            cartera_merge['siniestro_fecha_apert_reciente_otros'], format='%Y%m%d')

        cartera_merge['siniestro_fecha_apert_reciente_otros_m'] = cartera_merge['audit_fecha_cierre'].sub(
            cartera_merge['siniestro_fecha_apert_reciente_otros2'], axis=0)

        cartera_merge['siniestro_fecha_apert_reciente_otros_m'] = cartera_merge[
                                                                      'siniestro_fecha_apert_reciente_otros_m'] / np.timedelta64(
            1, 'D')

        cartera_merge['siniestro_fecha_apert_reciente_otros_m'] = (cartera_merge[
                                                                       'siniestro_fecha_apert_reciente_otros_m'] / 365) * 12

        cartera_merge = cartera_merge.drop(['siniestro_fecha_apert_reciente_auto2',
                                            'siniestro_fecha_apert_reciente_hogar2',
                                            'siniestro_fecha_apert_reciente_otros2',
                                            'siniestro_fecha_apert_reciente_auto',
                                            'siniestro_fecha_apert_reciente_hogar',
                                            'siniestro_fecha_apert_reciente_otros'
                                            ], 1)

        return cartera_merge

    def get_tenencia(Btenencia, cartera):
        """
        :param cartera: las polizas perteneciente a la cartera
        :return: Returna la cartera con las variables adicionales de las tenencias
        """

        fecha_target = cartera.iloc[0]['poliza_fecha_cartera2']
        pd.to_numeric(fecha_target, errors='coerce')
        #fecha_target = str(fecha_target)

        fecha_cierre = cartera.iloc[0]['audit_fecha_cierre_corta']
        pd.to_numeric(fecha_cierre, errors='coerce')
        #fecha_cierre = str(fecha_cierre)

        Btenencia['poliza_fecha_inicio_year'] = (Btenencia['poliza_fecha_inicio'] // 10000).astype(int)

        vigor = Btenencia[((Btenencia['poliza_fecha_inicio2'] <= fecha_cierre) & (fecha_cierre <= Btenencia['poliza_fecha_cambio_estado2']) & ((Btenencia['poliza_estado'].isin(['A', 'R'])))) | ((fecha_cierre <= Btenencia['poliza_fecha_vencimiento2']) & (Btenencia['poliza_estado'] == 'V') & (Btenencia['poliza_fecha_inicio2'] <= fecha_cierre))]

        fecha_cierre_year = fecha_cierre//100
        pd.to_numeric(fecha_cierre_year, errors='coerce')

        print("OJO:  Datatype, ", vigor['poliza_fecha_inicio_year'].dtype)
        print("OJO:  fecha_cierre_year, ", fecha_cierre_year)

        #   Selecciono solo el conjunto de polizas que estaban en Vigor en el momento del cierre
        vigor['poliza_vigor_year'] = vigor.apply(lambda x: Home.time_as_dates(x['poliza_fecha_inicio_year'], fecha_cierre_year), axis=1)

        vigor_numpol_total_polizas_vigor = vigor.groupby(['cod_global_unico']).size().reset_index(name='numpol_total_vigor')
        vigor_numpol_producto = vigor.groupby(['cod_global_unico', 'poliza_agrup_prod_cod']).size().reset_index(name='numpol_total_producto')

        t_vigor_numpol_producto_auto = vigor_numpol_producto[vigor_numpol_producto['poliza_agrup_prod_cod'] == 'AUTOS']
        t_vigor_numpol_producto_auto = t_vigor_numpol_producto_auto.rename(columns={'numpol_total_producto': 'numpol_auto'})

        t_vigor_numpol_producto_auto = t_vigor_numpol_producto_auto.drop(['poliza_agrup_prod_cod'], 1)

        t_vigor_numpol_producto_otros = vigor_numpol_producto[~(vigor_numpol_producto['poliza_agrup_prod_cod'].isin(['AUTOS', 'HOGAR']))]
        t_vigor_numpol_producto_otros = t_vigor_numpol_producto_otros.rename(columns={'numpol_total_producto': 'numpol_otros'})
        t_vigor_numpol_producto_otros = t_vigor_numpol_producto_otros.drop(['poliza_agrup_prod_cod'], 1)

        auto = vigor[vigor['poliza_agrup_prod_cod'] == 'AUTOS']

        auto['max_ant_vigor_auto'] = auto.groupby('cod_global_unico')['poliza_vigor_year'].transform('max')
        auto = auto.drop_duplicates(['cod_global_unico'], keep='first')

        t_auto_max = auto[['cod_global_unico', 'max_ant_vigor_auto']]

        otros = vigor[~(vigor['poliza_agrup_prod_cod'].isin(['AUTOS', 'HOGAR']))]
        otros['max_ant_vigor_otros'] = otros.groupby('cod_global_unico')['poliza_vigor_year'].transform('max')
        otros = otros.drop_duplicates(['cod_global_unico'], keep='first')
        t_otros_max = otros[['cod_global_unico', 'max_ant_vigor_otros']]

        vigor['max_ant_vigor_total'] = vigor.groupby('cod_global_unico')['poliza_vigor_year'].transform('max')
        t_vigor_max = vigor.drop_duplicates(['cod_global_unico'], keep='first')
        t_vigor_max = t_vigor_max[['cod_global_unico', 'max_ant_vigor_total']]

        cartera_merge = pd.merge(cartera, vigor_numpol_total_polizas_vigor, how='left', on='cod_global_unico')
        cartera_merge = pd.merge(cartera_merge, t_vigor_max, how='left', on='cod_global_unico')
        cartera_merge = pd.merge(cartera_merge, t_vigor_numpol_producto_auto, how='left', on='cod_global_unico')
        cartera_merge = pd.merge(cartera_merge, t_auto_max, how='left', on='cod_global_unico')
        cartera_merge = pd.merge(cartera_merge, t_vigor_numpol_producto_otros, how='left', on='cod_global_unico')
        cartera_merge = pd.merge(cartera_merge, t_otros_max, how='left', on='cod_global_unico')
        del auto, otros, t_vigor_max, t_otros_max, t_auto_max, t_vigor_numpol_producto_otros

        #   Limpiar valores raros como por ejemplo tener total en vigor en cero cuando existe numpol_hogar
        cartera_merge.loc[(cartera_merge['numpol_total_vigor'].isnull()), 'numpol_total_vigor'] = cartera_merge['numpol_hogar']
        cartera_merge.loc[(cartera_merge['max_ant_vigor_total'].isnull()), 'max_ant_vigor_total'] = cartera_merge['max_ant_vigor_hogar']

        #   Create the variable multipolicy
        cartera_merge['d_multipol'] = pd.Series(0, index=cartera_merge.index)
        cartera_merge.loc[cartera_merge['numpol_total_vigor'] > 1, 'd_multipol'] = 1

        #   Delete los null values, if it is null it is because there is no other policies, i.e. it should be zero
        cartera_merge.loc[(cartera_merge['numpol_auto'].isnull()), 'numpol_auto'] = 0
        #cartera_merge.loc[(cartera_merge['max_ant_vigor_auto'].isnull()), 'max_ant_vigor_auto'] = 0

        cartera_merge.loc[(cartera_merge['numpol_otros'].isnull()), 'numpol_otros'] = 0
        #cartera_merge.loc[(cartera_merge['max_ant_vigor_otros'].isnull()), 'max_ant_vigor_otros'] = 0

        return cartera_merge

    def get_clean_tenency_bottle(Btenencia, cartera):
        '''Aqui se trata de limpiar la botella de los falsos reemplazos y poder asi tenre una
        botella de tenencia limpia para poder contar adecuadamente los número de las anulaciones'''

        tenencia_A_H = Btenencia
        tenencia_A_H.loc[tenencia_A_H['poliza_estado'] == 'R', 'poliza_estado'] = 'A'

        # Escojo la cartera para sacar el conjunto de polizas de la tenencia
        cartera.sort_values(['poliza_id', 'poliza_version'], ascending=[True, False])
        cartera_id = cartera.drop_duplicates(['cod_global_unico'], keep='first', inplace=False)
        cartera_id = pd.DataFrame(cartera_id[['cod_global_unico']])

        tenencia_A_H = pd.merge(tenencia_A_H, cartera_id, how='inner', on='cod_global_unico')

        # Cambiando la botella de tenencia
        # Creo un subset de la tenencia para que no se tarde tanto y verificar que lo está haciendo bien, luego lo reemplazo por el DF original


        df_A = tenencia_A_H[(tenencia_A_H['poliza_estado'] == 'A')][
            ['cod_global_unico', 'poliza_id', 'poliza_estado', 'poliza_fecha_cambio_estado', 'poliza_fecha_inicio',
             'poliza_agrup_prod_cod','poliza_fecha_emision']]
        df_V_A = tenencia_A_H[(tenencia_A_H['poliza_estado'].isin(['V', 'A']))][
            ['cod_global_unico', 'poliza_id', 'poliza_estado', 'poliza_fecha_cambio_estado', 'poliza_fecha_inicio',
             'poliza_agrup_prod_cod','poliza_fecha_emision']]

        df_merge = pd.merge(left=df_A, right=df_V_A, left_on=['cod_global_unico', 'poliza_agrup_prod_cod'],
                            right_on=['cod_global_unico', 'poliza_agrup_prod_cod'])

        df_merge = df_merge[(df_merge['poliza_id_x'] != df_merge['poliza_id_y'])]

        df_merge = df_merge[(df_merge['poliza_fecha_inicio_x'] < df_merge['poliza_fecha_inicio_y'])]
        df_merge.loc[df_merge['poliza_fecha_cambio_estado_x'] == 0, 'poliza_fecha_cambio_estado_x'] = df_merge['poliza_fecha_emision_x']
        df_merge['poliza_fecha_cambio_estado_x'] = pd.to_datetime(df_merge['poliza_fecha_cambio_estado_x'],format='%Y%m%d')
        df_merge['poliza_fecha_inicio_y'] = pd.to_datetime(df_merge['poliza_fecha_inicio_y'], format='%Y%m%d')

        df_merge['difference'] = df_merge['poliza_fecha_inicio_y'].sub(df_merge['poliza_fecha_cambio_estado_x'], axis=0)

        df_merge['difference'] = df_merge['difference'] / np.timedelta64(1, 'D')


        df_reemplazos_found = df_merge[
            (df_merge['poliza_fecha_inicio_y'] >= df_merge['poliza_fecha_cambio_estado_x']) & (
            df_merge['difference'].abs() <= 60)]
        df_reemplazos_multiple = df_reemplazos_found.groupby(['poliza_id_y']).agg(
            {'poliza_id_x': 'count'}).reset_index().rename(columns={'poliza_id_x': 'count'})
        df_reemplazos_gt1 = df_reemplazos_multiple[df_reemplazos_multiple['count'] > 1]
        df_real_reemplazos = df_reemplazos_found[
            ~df_reemplazos_found['poliza_id_y'].isin(df_reemplazos_gt1.poliza_id_y)]
        df_reemplazos_multiple_2 = df_real_reemplazos.groupby(['poliza_id_x']).agg(
            {'poliza_id_y': 'count'}).reset_index().rename(columns={'poliza_id_y': 'count'})

        df_reemplazos_gt1_2 = df_reemplazos_multiple_2[df_reemplazos_multiple_2['count'] > 1]
        df_real_reemplazos_2 = df_real_reemplazos[
            ~df_real_reemplazos['poliza_id_x'].isin(df_reemplazos_gt1_2.poliza_id_x)]
        df_real_reemplazos_3 = df_real_reemplazos_2[['poliza_id_x', 'poliza_fecha_inicio_x', 'poliza_id_y']]

        tenencia_2 = pd.merge(left=tenencia_A_H, right=df_real_reemplazos_3, left_on=['poliza_id'],
                              right_on=['poliza_id_y'], how='left')

        # Change the fecha_inicio of the the replaced policy of the inicial policy
        tenencia_2['poliza_fecha_inicio'] = np.where(tenencia_2['poliza_fecha_inicio_x'].isnull(),
                                                     tenencia_2['poliza_fecha_inicio'],
                                                     tenencia_2['poliza_fecha_inicio_x'])

        # Delete the policies for which replacemnt is found
        tenencia_2 = tenencia_2[~tenencia_2['poliza_id'].isin(df_real_reemplazos_3.poliza_id_x)]
        tenencia_2 = tenencia_2.drop(['poliza_id_x', 'poliza_fecha_inicio_x', 'poliza_id_y'], 1)

        return (tenencia_2)

    def get_anulaciones(Btenencia, cartera):
        """
        :param cartera: las polizas perteneciente a la cartera
        :return: Returna la cartera con las variables adicionales de las tenencias
        """

        fecha_target = cartera.iloc[0]['poliza_fecha_cartera2']
        pd.to_numeric(fecha_target, errors='coerce')

        fecha_cierre = cartera.iloc[0]['audit_fecha_cierre_corta']
        pd.to_numeric(fecha_cierre, errors='coerce')


        # Debo separar anulaciones intencionales con las saneadas con intencion en los ultimos 12 meses

        fecha_cierre_year = fecha_cierre // 100
        fecha_cierre_ago = fecha_cierre - 200

        # Limpio los reemplazos no informados
        #Btenencia = Home.get_clean_tenency_bottle(Btenencia, cartera)

        anulaciones = Btenencia[(Btenencia['poliza_estado'].isin(['A'])) &
                                (fecha_cierre_ago <= Btenencia['poliza_fecha_cambio_estado2']) &
                                (Btenencia['poliza_fecha_cambio_estado2'] <= fecha_cierre)]

        # Marcar por cliente si ha tenido anulaciones por saneamiento d_saneada_anterior si el cliente ha tenido saneamiento
        saneamiento = ['165', '705']
        anulacion_voluntaria = anulaciones[~(anulaciones['poliza_motivo_cancelacion_codigo'].isin(saneamiento))]
        anulacion_saneadas = anulaciones[(anulaciones['poliza_motivo_cancelacion_codigo'].isin(saneamiento))]
        anul_saneadas = anulacion_saneadas.groupby(['cod_global_unico']).size().reset_index(
            name='numpol_saneada_anterior')
        anul_voluntarias = anulacion_voluntaria.groupby(['cod_global_unico']).size().reset_index(
            name='numpol_anul_total')


        # Anulaciones por saneamiento
        cartera_2 = pd.merge(cartera, anul_saneadas, how='left', on='cod_global_unico')
        cartera_2.loc[cartera_2['numpol_saneada_anterior'].isnull(), 'numpol_saneada_anterior'] = 0
        cartera_2['d_saneada_anterior'] = pd.Series(0, index=cartera_2.index)
        cartera_2.loc[cartera_2['numpol_saneada_anterior'] > 0, 'd_saneada_anterior'] = 1

        # Anulaciones voluntarias
        cartera_2 = pd.merge(cartera_2, anul_voluntarias, how='left', on='cod_global_unico')

        # Aqui vemos la fecha mas reciente de anulacion en cualquier producto que tiene contratado

        anulacion_voluntaria['fecha_anul_reciente_total'] = anulacion_voluntaria.groupby('cod_global_unico')[
            'poliza_fecha_cambio_estado'].transform('max')
        t_anulacion_voluntaria = anulacion_voluntaria.drop_duplicates(['cod_global_unico'], keep='first')
        t_anulacion_voluntaria = t_anulacion_voluntaria[['cod_global_unico', 'fecha_anul_reciente_total']]
        #print(t_anulacion_voluntaria.head())

        cartera_2 = pd.merge(cartera_2, t_anulacion_voluntaria, how='left', on='cod_global_unico')

        # Es una variable categorica con 3 valores 1:Si hubo anulacion, 0: No hubo anulacion pero si tenia otros productos
        # 2: No hubo anulacion porque no habia otros productos

        cartera_2['d_churn_voluntario'] = pd.Series("2", index=cartera_2.index)
        cartera_2.loc[
            ((cartera_2['d_multipol'] == 1) & (cartera_2['numpol_anul_total'].isnull())), 'd_churn_voluntario'] = "0"
        cartera_2.loc[cartera_2['numpol_anul_total'] > 0, 'd_churn_voluntario'] = "1"



        hogar = anulacion_voluntaria[anulacion_voluntaria['poliza_agrup_prod_cod'] == 'HOGAR']
        auto = anulacion_voluntaria[anulacion_voluntaria['poliza_agrup_prod_cod'] == 'AUTOS']
        otros = anulacion_voluntaria[~(anulacion_voluntaria['poliza_agrup_prod_cod'].isin(['AUTOS', 'HOGAR']))]

        hogar_anul_saneadas = hogar.groupby(['cod_global_unico']).size().reset_index(name='numpol_anul_hogar')
        auto_anul_saneadas = auto.groupby(['cod_global_unico']).size().reset_index(name='numpol_anul_auto')
        otros_anul_saneadas = otros.groupby(['cod_global_unico']).size().reset_index(name='numpol_anul_otros')

        # Agregar las anulaciones separadas por producto
        cartera_2 = pd.merge(cartera_2, hogar_anul_saneadas, how='left', on='cod_global_unico')
        cartera_2 = pd.merge(cartera_2, auto_anul_saneadas, how='left', on='cod_global_unico')
        cartera_2 = pd.merge(cartera_2, otros_anul_saneadas, how='left', on='cod_global_unico')

        # crear las variables que tengan sentido y eliminar las otras para quitarme las NaN

        cartera_2['d_anul_hogar'] = pd.Series("2", index=cartera_2.index)
        cartera_2.loc[
            ((cartera_2['numpol_hogar'] > 1) & (cartera_2['numpol_anul_hogar'].isnull())), 'd_anul_hogar'] = "0"
        cartera_2.loc[cartera_2['numpol_anul_hogar'] > 0, 'd_anul_hogar'] = "1"

        cartera_2['d_anul_auto'] = pd.Series("2", index=cartera_2.index)
        cartera_2.loc[((cartera_2['numpol_auto'] > 1) & (cartera_2['numpol_anul_auto'].isnull())), 'd_anul_auto'] = "0"
        cartera_2.loc[cartera_2['numpol_anul_auto'] > 0, 'd_anul_auto'] = "1"

        cartera_2['d_anul_otros'] = pd.Series("2", index=cartera_2.index)
        cartera_2.loc[
            ((cartera_2['numpol_otros'] > 1) & (cartera_2['numpol_anul_otros'].isnull())), 'd_anul_otros'] = "0"
        cartera_2.loc[cartera_2['numpol_anul_otros'] > 0, 'd_anul_otros'] = "1"

        cartera_2 = cartera_2.drop(['numpol_anul_hogar', 'numpol_anul_auto', 'numpol_anul_otros'], 1)


        return cartera_2

    def get_premium(Bpremium, cartera):
        """
        :param cartera: las polizas perteneciente a la cartera
        :return: Returna la cartera con las variablesDE primas
        """
        cartera = cartera.drop_duplicates(['poliza_id'], keep='first')

        fecha_target = cartera.iloc[0]['poliza_fecha_cartera2']
        pd.to_numeric(fecha_target, errors='coerce')
        # fecha_target = str(fecha_target)

        fecha_cierre = cartera.iloc[0]['audit_fecha_cierre_corta']
        pd.to_numeric(fecha_cierre, errors='coerce')
        # fecha_cierre = str(fecha_cierre)

        fecha_cierre_ago = fecha_cierre - 100

        fecha_cierre_year = fecha_cierre // 100
        pd.to_numeric(fecha_cierre_year, errors='coerce')
        fecha_cierre_ago = fecha_cierre - 100
        fecha_target_1ago = fecha_target - 100
        fecha_target_2ago = fecha_target - 200
        fecha_target_1year = fecha_target + 100
        pd.to_numeric(fecha_cierre_year, errors='coerce')

        Bpremium['poliza_fecha_vencimiento_mov2'] = (Bpremium['poliza_fecha_vencimiento_mov'] // 100).astype(int)
        Bpremium['poliza_fecha_vto_nat2'] = (Bpremium['poliza_fecha_vto_nat'] // 100).astype(int)

        Bpremium_3years = Bpremium[(Bpremium['poliza_fecha_vto_nat2'] >= fecha_target_2ago) & (
        Bpremium['poliza_fecha_vto_nat2'] <= fecha_target_1year)]

        p_hogar = Bpremium_3years[Bpremium_3years['polizaAgrupProdDesc'] == 'HOGAR']
        p_otros = Bpremium_3years[~(Bpremium_3years['polizaAgrupProdDesc'].isin(['HOGAR']))]

        p1_hogar = p_hogar[(p_hogar['poliza_movimiento_desc'].isin(['CARTERA', 'CARTERA MANUAL'])) & (
        p_hogar['poliza_fecha_emision2'] >= fecha_cierre) & (p_hogar['poliza_fecha_emision2'] <= fecha_target)]
        p1_hogar['date_recent'] = p1_hogar.groupby('poliza_id')['poliza_fecha_emision'].transform('max')
        p1_hogar = p1_hogar.sort_values(['poliza_id', 'poliza_fecha_emision'], ascending=[True, False])

        p2_hogar = p_hogar[
            (p_hogar['poliza_fecha_vto_nat2'] <= fecha_target) & (p_hogar['poliza_fecha_emision2'] <= fecha_cierre)]
        p3_hogar = p_hogar[(p_hogar['poliza_fecha_vto_nat2'] <= fecha_target_1ago)]

        p2_hogar['date_recent'] = p2_hogar.groupby('poliza_id')['poliza_fecha_emision'].transform('max')
        p2_hogar = p2_hogar[p2_hogar['poliza_fecha_emision'] == p2_hogar['date_recent']]

        p3_hogar['date_recent'] = p3_hogar.groupby('poliza_id')['poliza_fecha_emision'].transform('max')
        p3_hogar = p3_hogar[p3_hogar['poliza_fecha_emision'] == p3_hogar['date_recent']]

        t_p1_hogar = p1_hogar.drop_duplicates(['poliza_id'], keep='first')
        t_p1_hogar = t_p1_hogar[
            ['poliza_id', 'poliza_prima_recibo', 'poliza_prima_bonus', 'poliza_prima_recargodescuento']]
        t_p1_hogar = t_p1_hogar.rename(
            columns={'poliza_prima_recibo': 'p1', 'poliza_prima_recargodescuento': 'p1_descuento',
                     'poliza_prima_bonus': 'p1_bonus'})

        t_p2_hogar = p2_hogar.drop_duplicates(['poliza_id'], keep='first')
        t_p2_hogar = t_p2_hogar[
            ['poliza_id', 'poliza_prima_recibo', 'poliza_prima_bonus', 'poliza_prima_recargodescuento']]
        t_p2_hogar = t_p2_hogar.rename(
            columns={'poliza_prima_recibo': 'p2', 'poliza_prima_recargodescuento': 'p2_descuento',
                     'poliza_prima_bonus': 'p2_bonus'})

        t_p3_hogar = p3_hogar.drop_duplicates(['poliza_id'], keep='first')
        t_p3_hogar = t_p3_hogar[
            ['poliza_id', 'poliza_prima_recibo', 'poliza_prima_bonus', 'poliza_prima_recargodescuento']]
        t_p3_hogar = t_p3_hogar.rename(
            columns={'poliza_prima_recibo': 'p3', 'poliza_prima_recargodescuento': 'p3_descuento',
                     'poliza_prima_bonus': 'p3_bonus'})

        cartera_merge = pd.merge(cartera, t_p1_hogar, how='left', on='poliza_id')
        cartera_merge = pd.merge(cartera_merge, t_p2_hogar, how='left', on='poliza_id')
        cartera_merge = pd.merge(cartera_merge, t_p3_hogar, how='left', on='poliza_id')

        cartera_merge = cartera_merge[~(cartera_merge['p1'].isnull())]
        cartera_merge = cartera_merge[~(cartera_merge['p2'].isnull())]

        # Number of suplements in the last 2 years

        Bpremium_1years = Bpremium[(Bpremium['poliza_fecha_vto_nat2'] >= fecha_target) & (
        Bpremium['poliza_fecha_vto_nat2'] <= fecha_target_1year)]
        Bpremium_2years = Bpremium[(Bpremium['poliza_fecha_vto_nat2'] >= fecha_target_1ago) & (
        Bpremium['poliza_fecha_vto_nat2'] <= fecha_target)]

        p_hogar_2year = Bpremium_2years[(Bpremium_2years['polizaAgrupProdDesc'] == 'HOGAR') & (
        Bpremium_2years['poliza_movimiento_desc'] == 'SUPLEMENTO')]
        p_hogar_1year = Bpremium_1years[(Bpremium_1years['polizaAgrupProdDesc'] == 'HOGAR') & (
        Bpremium_1years['poliza_movimiento_desc'] == 'SUPLEMENTO')]

        poliza_num_suplementos_2year = p_hogar_2year.groupby(['poliza_id']).size().reset_index(
            name='poliza_num_suplementos_2year')
        poliza_num_suplementos_1year = p_hogar_1year.groupby(['poliza_id']).size().reset_index(
            name='poliza_num_suplementos_1year')

        cartera_merge = pd.merge(cartera_merge, poliza_num_suplementos_2year, how='left', on='poliza_id')
        cartera_merge = pd.merge(cartera_merge, poliza_num_suplementos_1year, how='left', on='poliza_id')

        cartera_merge.loc[cartera_merge['poliza_num_suplementos_1year'].isnull(), 'poliza_num_suplementos_1year'] = 0
        cartera_merge.loc[cartera_merge['poliza_num_suplementos_2year'].isnull(), 'poliza_num_suplementos_2year'] = 0

        return cartera_merge

    def output_hogar_processed(file_df):

        file_name = PATH_VAR.path_out2 + "processed_hogar.csv"

        file_df.to_csv(file_name, sep=';', header=True, index=False, quotechar='"')

    def construct_claims_response_time(df_5_1, df_claim_initial, df_claim_guarantees):

        df_5_1['audit_fecha_cierre'] = df_5_1['audit_fecha_cierre'].astype(str)
        df_5_1['bottle_date'] = df_5_1['audit_fecha_cierre'].str.replace('-', '').apply(int)
        df_5_1['bottle_date'] = pd.to_datetime(df_5_1['bottle_date'], format='%Y%m%d')

        df_5_1['last_year'] = df_5_1.bottle_date - pd.DateOffset(years=1)
        df_5_1['last_year'] = df_5_1['last_year'].astype('str')

        df_5_1['bottle_date'] = df_5_1['bottle_date'].astype('str')

        df_5_1['bottle_date'] = df_5_1['bottle_date'].str.replace('-', '').apply(int)
        df_5_1['last_year'] = df_5_1['last_year'].str.replace('-', '').apply(int)

        bottle_date = df_5_1['bottle_date'].iloc[0]
        last_year = df_5_1['last_year'].iloc[0]

        # take active policies
        df_active_pol = df_5_1[['poliza_id', 'cod_global_unico']]

        # take claim_guarantees of only active policies
        df_claim_guarantees_active = df_claim_guarantees[df_claim_guarantees.poliza_id.isin(df_active_pol.poliza_id)]

        # load claim data
        df_claim_initial = df_claim_initial

        # contruct correct variable for rejected claims from siniestro_rehuse
        df_claim_initial['siniestro_rejected'] = np.where((df_claim_initial['siniestro_rehuse'] == True), 1, 0)

        # take only required fields from claim data
        df_claim_active = df_claim_initial[
            ['poliza_id', 'siniestro_id', 'siniestro_fecha_apertura', 'siniestro_fecha_ocurrencia',
             'siniestro_fecha_fin', 'siniestro_rejected', 'siniestro_estado_siniestro']][
            df_claim_initial.poliza_id.isin(df_active_pol.poliza_id)]

        # merge claim guarantee with claims
        df_claim_guarantee_merged = pd.merge(left=df_claim_guarantees_active, right=df_claim_active,
                                             left_on=['siniestro_id'], right_on=['siniestro_id'], how='left')

        # take only those claims where the finish date is in  last one year

        df_claim_guarantees_1ano = df_claim_guarantee_merged[
            (df_claim_guarantee_merged['siniestro_fecha_fin'] > last_year) & (
                df_claim_guarantee_merged['siniestro_fecha_fin'] <= bottle_date)]

        # seperate the pago and the gato movements because the way to calculate the claim response time for these two is different.
        # for pago we calculate the time when the money was paid but in gato we calculate the time when the service was provided

        ########################################################################################
        # calculations for pago

        # 1. pago movements are the one where the pago fecha emission is not 0
        df_claim_pago = df_claim_guarantees_1ano[df_claim_guarantees_1ano['po_pago_emision'] != 0]

        # 2. the total amount paid to the customer is pago indemnizacion ( direct payment) + pago_factura (reciept payment)
        df_claim_pago['po_pago_amount'] = df_claim_pago['po_pago_indemnizacion_importe_neto'] + df_claim_pago[
            'po_pago_factura_importe_neto']

        # 3. we need to exclude rejected coverages this is handles in two steps.
        # step1 : find the movements where pago anulacion is 1. These are rejected and need to be excluded
        df_anulacion_pagos = df_claim_pago[df_claim_pago['po_pago_es_anulacion'] == 1][
            ['siniestro_id', 'po_res_cobertura', 'po_pago_amount', 'po_res_garantia']]

        # also exclude the movements where the anulacion is not null but the total amount matches with the movement where anulacion is 1 for the same claim and coverage
        df_claim_pagos_v1 = pd.merge(left=df_claim_pago, right=df_anulacion_pagos,
                                     left_on=['siniestro_id', 'po_res_cobertura', 'po_pago_amount'],
                                     right_on=['siniestro_id', 'po_res_cobertura', 'po_pago_amount'], how='left')
        df_claim_pagos_v2 = df_claim_pagos_v1[df_claim_pagos_v1['po_res_garantia_y'].isnull()]

        # step 2 exclude the movements where rehusado is 1
        df_claim_pagos_v3 = df_claim_pagos_v2[df_claim_pagos_v2['po_pago_rehusado'] == 0]

        # 4. only include movements where both indemnizacion amount and  factura amount is not less that zero
        df_claim_pagos_v4 = df_claim_pagos_v3[(df_claim_pagos_v3['po_pago_indemnizacion_importe_neto'] >= 0) & (
            df_claim_pagos_v3['po_pago_factura_importe_neto'] >= 0)]

        # 5. only include movements where the total amount is greater than zero
        df_claim_pagos_v5 = df_claim_pagos_v4[(df_claim_pagos_v4['po_pago_amount'] > 0)]

        # 6. only include movements where the pago fecha emision is less than the bottle_date
        bottle_date_int = bottle_date
        df_claim_pagos_v6 = df_claim_pagos_v5[(df_claim_pagos_v5['po_pago_emision'] <= bottle_date_int)]

        # 7. from all the movements of a coverage find the max of fecha emission. This the time when the pago of the coverage was resolved
        df_claim_pagos_v7 = df_claim_pagos_v6.groupby(['siniestro_id', 'po_res_cobertura']).agg(
            {'po_pago_emision': 'max'}).reset_index()
        # in the end we get the response time per claim, coverage

        # 8.merge pago final data with the claims level data of active policies
        df_claim_pagos_fnl = pd.merge(left=df_claim_pagos_v7, right=df_claim_active, left_on=['siniestro_id'],
                                      right_on=['siniestro_id'], how='left')

        # 9. calculate the difference in pago fecha emission and the date of occurence of the claim
        df_claim_pagos_fnl['po_pago_emision'] = pd.to_datetime(df_claim_pagos_fnl['po_pago_emision'], format='%Y%m%d')
        df_claim_pagos_fnl['siniestro_fecha_ocurrencia'] = pd.to_datetime(
            df_claim_pagos_fnl['siniestro_fecha_ocurrencia'], format='%Y%m%d')

        df_claim_pagos_fnl['num_days_resolve'] = df_claim_pagos_fnl['po_pago_emision'] - df_claim_pagos_fnl[
            'siniestro_fecha_ocurrencia']
        df_claim_pagos_fnl['num_days_resolve'] = df_claim_pagos_fnl['num_days_resolve'] / np.timedelta64(1, 'D')

        # 10. take only the required filed from the final dataset : df_claim_pagos_fnl
        df_claim_pagos_fnl = df_claim_pagos_fnl[
            ['siniestro_id', 'po_res_cobertura', 'po_pago_emision', 'num_days_resolve']]

        ##################################################################
        # calculation for gasto movements

        # gasto movements are the one where the gasto factura_fecha_emision is not 0
        df_claim_gasto = df_claim_guarantees_1ano[df_claim_guarantees_1ano['po_gasto_factura_fecha_emision'] != 0]

        # the total amount paid to the service is gasto indemnizacion ( direct payment) + gasto_factura (reciept payment)
        df_claim_gasto['po_gasto_amount'] = df_claim_gasto['po_gasto_indemnizacion_importe_neto'] + df_claim_gasto[
            'po_gasto_factura_importe_neto']

        # 3. we need to exclude rejected coverages this is handles in two steps.
        # step1 : find the movements where gasto anulacion is 1. These are rejected and need to be excluded
        df_anulacion_gasto = df_claim_gasto[df_claim_gasto['po_gasto_es_anulacion'] == 1][
            ['siniestro_id', 'po_res_cobertura', 'po_gasto_amount', 'po_res_garantia']]

        # also exclude the movements where the anulacion is not null but the total amount matches with the movement where anulacion is 1 for the same claim and coverage
        df_claim_gasto_v1 = pd.merge(left=df_claim_gasto, right=df_anulacion_gasto,
                                     left_on=['siniestro_id', 'po_res_cobertura', 'po_gasto_amount'],
                                     right_on=['siniestro_id', 'po_res_cobertura', 'po_gasto_amount'], how='left')
        df_claim_gasto_v2 = df_claim_gasto_v1[df_claim_gasto_v1['po_res_garantia_y'].isnull()]

        # step 2 exclude the movements where rehusado is 1
        df_claim_gasto_v3 = df_claim_gasto_v2[df_claim_gasto_v2['po_pago_rehusado'] == 0]

        # 4. only include movements where both indemnizacion amount and  factura amount is not less that zero
        df_claim_gasto_v4 = df_claim_gasto_v3[(df_claim_gasto_v3['po_gasto_indemnizacion_importe_neto'] >= 0) & (
            df_claim_gasto_v3['po_gasto_factura_importe_neto'] >= 0)]

        # 5. only include movements where the total amount is greater than zero
        df_claim_gasto_v5 = df_claim_gasto_v4[df_claim_gasto_v4['po_gasto_amount'] > 0]

        # 6. only include movements where the pago fecha emision is less than the bottle_date and greater than 20100131
        # 20100131 limit is included because there are some wrong dates in the gasto_fractura_emision like 900005 so to avoid those
        df_claim_gasto_v6 = df_claim_gasto_v5[
            (df_claim_gasto_v5['po_gasto_factura_fecha_emision'] <= bottle_date_int) & (
                df_claim_gasto_v5['po_gasto_factura_fecha_emision'] > 20100131)]

        # 7. from all the movements of a coverage find the max of fecha emission. This the time when the pago of the coverage was resolved
        df_claim_gasto_v7 = df_claim_gasto_v6.groupby(['siniestro_id', 'po_res_cobertura']).agg(
            {'po_gasto_factura_fecha_emision': 'max'}).reset_index()

        # 8.merge pago final data with the claims level data of active policies
        df_claim_gasto_fnl = pd.merge(left=df_claim_gasto_v7, right=df_claim_active, left_on=['siniestro_id'],
                                      right_on=['siniestro_id'], how='left')

        # 9. calculate the difference in pago fecha emission and the date of occurence of the claim
        df_claim_gasto_fnl['po_gasto_factura_fecha_emision'] = pd.to_datetime(
            df_claim_gasto_fnl['po_gasto_factura_fecha_emision'], format='%Y%m%d')
        df_claim_gasto_fnl['siniestro_fecha_ocurrencia'] = pd.to_datetime(
            df_claim_gasto_fnl['siniestro_fecha_ocurrencia'], format='%Y%m%d')

        df_claim_gasto_fnl['num_days_resolve'] = df_claim_gasto_fnl['po_gasto_factura_fecha_emision'] - \
                                                 df_claim_gasto_fnl['siniestro_fecha_ocurrencia']
        df_claim_gasto_fnl['num_days_resolve'] = df_claim_gasto_fnl['num_days_resolve'] / np.timedelta64(1, 'D')

        # 10. take only the required filed from the final dataset : df_claim_gasto_fnl
        df_claim_gasto_fnl = df_claim_gasto_fnl[
            ['siniestro_id', 'po_res_cobertura', 'po_gasto_factura_fecha_emision', 'num_days_resolve']]

        ####################################################################
        # merge and aggregate all the data

        # 1.list all the claims, coverages and their date of occurence
        df_claim_guarantees_list = df_claim_guarantees_1ano[
            ['siniestro_id', 'po_res_cobertura', 'siniestro_fecha_ocurrencia']].drop_duplicates()

        # 2. merge the pago and gasto data with total claims and coverage data
        df_claim_merge_v1 = pd.merge(left=df_claim_guarantees_list, right=df_claim_pagos_fnl,
                                     left_on=['siniestro_id', 'po_res_cobertura'],
                                     right_on=['siniestro_id', 'po_res_cobertura'], how='left')
        df_claim_merge_v2 = pd.merge(left=df_claim_merge_v1, right=df_claim_gasto_fnl,
                                     left_on=['siniestro_id', 'po_res_cobertura'],
                                     right_on=['siniestro_id', 'po_res_cobertura'], how='left')

        # 3. only consider those claims-coverage where we have atleast either pago or gasto movement
        df_claim_merge_v3 = df_claim_merge_v2[
            (df_claim_merge_v2['num_days_resolve_x'].notnull()) | (df_claim_merge_v2['num_days_resolve_y'].notnull())]

        # 4. fill null with 0 for pago numdays resolve and gasto numdays resolve
        df_claim_merge_v3.num_days_resolve_x.fillna(0, inplace=True)
        df_claim_merge_v3.num_days_resolve_y.fillna(0, inplace=True)

        # 5.calculate the max of numdays resolve pago and gasto for a claim-coverage
        df_claim_merge_v3['num_days_resolve_max'] = df_claim_merge_v3[['num_days_resolve_x', 'num_days_resolve_y']].max(
            axis=1)

        # 6. calculate the median value to resolve a claim per coverage
        df_coverage_median = df_claim_merge_v3.groupby(['po_res_cobertura']).agg(
            {'num_days_resolve_max': 'median'}).reset_index().rename(
            columns={'num_days_resolve_max': 'median_num_days_resolve'})

        # 7.merge the claim-coverage-responsetime data with the median value of each coverage
        df_claim_merge_v4 = pd.merge(left=df_claim_merge_v3, right=df_coverage_median, left_on=['po_res_cobertura'],
                                     right_on=['po_res_cobertura'], how='left')

        # 8. calculate the difference in response time of claim-coverage wit the median of coverage
        df_claim_merge_v4['difference_response_time'] = (df_claim_merge_v4['num_days_resolve_max'] - df_claim_merge_v4[
            'median_num_days_resolve']) / (df_claim_merge_v4['median_num_days_resolve'] + 1)

        # 9.aggregate the data at claims level by calculating mean and max
        df_claim_merge_v5 = df_claim_merge_v4.groupby(['siniestro_id']).agg(
            {'difference_response_time': 'mean', }).reset_index().rename(
            columns={'difference_response_time': 'mean_difference_response_time'})
        df_claim_merge_v5_1 = df_claim_merge_v4.groupby(['siniestro_id']).agg(
            {'difference_response_time': 'max'}).reset_index().rename(
            columns={'difference_response_time': 'max_difference_response_time'})

        # 10. merge the mean and max by claims
        df_claim_merge_v6 = pd.merge(left=df_claim_merge_v5, right=df_claim_merge_v5_1, left_on=['siniestro_id'],
                                     right_on=['siniestro_id'])

        # 11. take required fields from claims of active policy - so that we get policy-claim pair
        df_claim_active_poliza = df_claim_active[['poliza_id', 'siniestro_id']]

        # 12. merge the mean-max by claim with the policy-claim
        df_claim_merge_v7 = pd.merge(left=df_claim_merge_v6, right=df_claim_active_poliza, left_on=['siniestro_id'],
                                     right_on=['siniestro_id'], how='left')

        # 13. aggreate the data at policy level so we get mean-max at policy level
        df_claim_merge_v8 = df_claim_merge_v7.groupby(['poliza_id']).agg({'mean_difference_response_time': 'mean', 'max_difference_response_time': 'max'}).reset_index()

        return df_claim_merge_v8

    def get_claim_response_time(cartera, df_claim_initial, df_claim_guarantees):
        df_claim_response_time = Home.construct_claims_response_time(cartera, df_claim_initial, df_claim_guarantees)
        df = pd.merge(left=cartera, right=df_claim_response_time, left_on=['poliza_id'], right_on=['poliza_id'], how='left')
        df.mean_difference_response_time.fillna(0, inplace=True)
        df.max_difference_response_time.fillna(0, inplace=True)
        return df

    def clean_reemplazos_other(df_5_3):

        # this function find reemplazos except hogar and auto . Reemplazos is the case where one policy is churned and replaced by another
        # policy.. There are two things that need to be done - 1. change the start date of new policy to start date of old policy. 2. Delete the old policy
        # also note that the new policy can be in V or A status as of now. Therefore while looking for reemplzas match A with V and also A with A


        # 1. put the policy data of inactive policies of all products except Auto and Hpgar in df_A. This represents are old policies
        df_A = df_5_3[(df_5_3['poliza_estado'] == 'A')][
            ['cod_global_unico', 'poliza_id', 'poliza_estado', 'poliza_fecha_cambio_estado', 'poliza_fecha_inicio',
             'poliza_agrup_prod_cod', 'poliza_fecha_emision']]

        # 2. put the policy data of active and inacitve policies of all products except Auto and Hpgar in df_V_A. This represent  new policies
        df_V_A = df_5_3[(df_5_3['poliza_estado'].isin(['V', 'A']))][
            ['cod_global_unico', 'poliza_id', 'poliza_estado', 'poliza_fecha_cambio_estado', 'poliza_fecha_inicio',
             'poliza_agrup_prod_cod', 'poliza_fecha_emision']]

        # 3. merge the df_A and df_V_A based on client code and the product
        df_merge_reemplazos = pd.merge(left=df_A, right=df_V_A, left_on=['cod_global_unico', 'poliza_agrup_prod_cod'],
                                       right_on=['cod_global_unico', 'poliza_agrup_prod_cod'])

        # 4. filter in such a way that we only get row where policy_id of old policy matched with policy_id of new policy
        df_merge_reemplazos = df_merge_reemplazos[
            (df_merge_reemplazos['poliza_id_x'] != df_merge_reemplazos['poliza_id_y'])]
        df_merge_reemplazos.loc[
            df_merge_reemplazos['poliza_fecha_cambio_estado_x'] == 0, 'poliza_fecha_cambio_estado_x'] = \
        df_merge_reemplazos['poliza_fecha_emision_x']

        # 5. filter again so that we only get rows where the start date of new policy is greater than start date of old policy
        df_merge_reemplazos = df_merge_reemplazos[
            (df_merge_reemplazos['poliza_fecha_inicio_y'] > df_merge_reemplazos['poliza_fecha_inicio_x'])]

        # 6. calculate the difference in the churn date of the old policy and start date of the new policy
        df_merge_reemplazos['poliza_fecha_cambio_estado_x'] = pd.to_datetime(
            df_merge_reemplazos['poliza_fecha_cambio_estado_x'], format='%Y%m%d')
        df_merge_reemplazos['poliza_fecha_inicio_y'] = pd.to_datetime(df_merge_reemplazos['poliza_fecha_inicio_y'],
                                                                      format='%Y%m%d')
        df_merge_reemplazos['difference'] = df_merge_reemplazos['poliza_fecha_inicio_y'].sub(
            df_merge_reemplazos['poliza_fecha_cambio_estado_x'], axis=0)
        df_merge_reemplazos['difference'] = df_merge_reemplazos['difference'] / np.timedelta64(1, 'D')

        # 7. reemplazos are those where the difference is between +- 60 days . Sometime it could be negative because they update the churn date of the
        # old policy later in time.
        df_reemplazos_found = df_merge_reemplazos[
            (df_merge_reemplazos['difference'] >= -60) & (df_merge_reemplazos['difference'] <= 60)]

        # 8. because we didn´t match with the object information while looking for replacements , there could be posibility that
        # multiple new policies are found for one old or multiple old policies are found for one new policy. We have to find both cases.

        # 8.1 case where multiple old policies are found for one new policy
        df_reemplazos_multiple_1 = df_reemplazos_found.groupby(['poliza_id_y']).agg(
            {'poliza_id_x': 'count'}).reset_index().rename(columns={'poliza_id_x': 'count'})
        df_reemplazos_gt1 = df_reemplazos_multiple_1[df_reemplazos_multiple_1['count'] > 1]

        # 8.2 delete 'multiple old policies are found for one new policy' from df_reemplazos_found
        df_real_reemplazos = df_reemplazos_found[
            ~df_reemplazos_found['poliza_id_y'].isin(df_reemplazos_gt1.poliza_id_y)]

        # 8.3 case where multiple new policies are found for one old policy
        df_reemplazos_multiple_2 = df_real_reemplazos.groupby(['poliza_id_x']).agg(
            {'poliza_id_y': 'count'}).reset_index().rename(columns={'poliza_id_y': 'count'})
        df_reemplazos_gt1_2 = df_reemplazos_multiple_2[df_reemplazos_multiple_2['count'] > 1]

        # 8.4 delete 'multiple new policies are found for one old policy' from df_real_reemplazos
        df_real_reemplazos_2 = df_real_reemplazos[
            ~df_real_reemplazos['poliza_id_x'].isin(df_reemplazos_gt1_2.poliza_id_x)]

        # 9 pick only the required columns from the dataset ie old policy, new policy , start date of old policy
        df_real_reemplazos_3 = df_real_reemplazos_2[['poliza_id_x', 'poliza_fecha_inicio_x', 'poliza_id_y']]

        # 10. implement reeplacement

        # 10.1  take the original dataset and merge it with the final reemplazos dataset by the new policy_id. This gives us all information
        # about the new policy
        df_5_4 = pd.merge(left=df_5_3, right=df_real_reemplazos_3, left_on=['poliza_id'], right_on=['poliza_id_y'],
                          how='left')

        # 10.2 Change the start of the the new policy with the start date of the old policy
        df_5_4['poliza_fecha_inicio'] = np.where(df_5_4['poliza_fecha_inicio_x'].isnull(),
                                                 df_5_4['poliza_fecha_inicio'], df_5_4['poliza_fecha_inicio_x'])

        # 10.3 Delete the old policies
        df_5_5 = df_5_4[~df_5_4['poliza_id'].isin(df_real_reemplazos_3.poliza_id_x)]

        # df_5_5 will be your new cleaned "policy bottle"


        # 11.delete not required columns
        del df_5_5['poliza_id_x']
        #
        del df_5_5['poliza_fecha_inicio_x']
        #
        del df_5_5['poliza_id_y']

        # 12. change the dtype of poliza_fecha_inicio
        df_5_5['poliza_fecha_inicio'] = df_5_5.poliza_fecha_inicio.astype(int)

        return df_5_5

    def clean_reemplazos_home(df_5_3):

        '''
         this function find reemplazos HOGAR . Reemplazos is the case where one policy is churned and replaced by another
         policy.. There are two things that need to be done - 1. change the start date of new policy to start date of old policy. 2. Delete the old policy
         also note that the new policy can be in V or A status as of now. Therefore while looking for reemplzas match A with V and also A with A
        :return: clean bottle with not replacement
        '''




        # 1. put the policy data of inactive policies of all products except Auto and Hpgar in df_A. This represents are old policies
        df_A = df_5_3[(df_5_3['poliza_estado'] == 'A')][
            ['cod_global_unico', 'poliza_id', 'poliza_estado', 'poliza_fecha_cambio_estado', 'poliza_fecha_inicio',
             'poliza_agrup_prod_cod', 'poliza_fecha_emision']]

        # 2. put the policy data of active and inacitve policies of all products except Auto and Hpgar in df_V_A. This represent  new policies
        df_V_A = df_5_3[(df_5_3['poliza_estado'].isin(['V', 'A']))][
            ['cod_global_unico', 'poliza_id', 'poliza_estado', 'poliza_fecha_cambio_estado', 'poliza_fecha_inicio',
             'poliza_agrup_prod_cod', 'poliza_fecha_emision']]

        # 3. merge the df_A and df_V_A based on client code and the product
        df_merge_reemplazos = pd.merge(left=df_A, right=df_V_A, left_on=['cod_global_unico', 'poliza_agrup_prod_cod'],
                                       right_on=['cod_global_unico', 'poliza_agrup_prod_cod'])

        # 4. filter in such a way that we only get row where policy_id of old policy matched with policy_id of new policy
        df_merge_reemplazos = df_merge_reemplazos[
            (df_merge_reemplazos['poliza_id_x'] != df_merge_reemplazos['poliza_id_y'])]
        df_merge_reemplazos.loc[
            df_merge_reemplazos['poliza_fecha_cambio_estado_x'] == 0, 'poliza_fecha_cambio_estado_x'] = \
        df_merge_reemplazos['poliza_fecha_emision_x']

        # 5. filter again so that we only get rows where the start date of new policy is greater than start date of old policy
        df_merge_reemplazos = df_merge_reemplazos[
            (df_merge_reemplazos['poliza_fecha_inicio_y'] > df_merge_reemplazos['poliza_fecha_inicio_x'])]

        # 6. calculate the difference in the churn date of the old policy and start date of the new policy
        df_merge_reemplazos['poliza_fecha_cambio_estado_x'] = pd.to_datetime(
            df_merge_reemplazos['poliza_fecha_cambio_estado_x'], format='%Y%m%d')
        df_merge_reemplazos['poliza_fecha_inicio_y'] = pd.to_datetime(df_merge_reemplazos['poliza_fecha_inicio_y'],
                                                                      format='%Y%m%d')
        df_merge_reemplazos['difference'] = df_merge_reemplazos['poliza_fecha_inicio_y'].sub(
            df_merge_reemplazos['poliza_fecha_cambio_estado_x'], axis=0)
        df_merge_reemplazos['difference'] = df_merge_reemplazos['difference'] / np.timedelta64(1, 'D')

        # 7. reemplazos are those where the difference is between +- 60 days . Sometime it could be negative because they update the churn date of the
        # old policy later in time.
        df_reemplazos_found = df_merge_reemplazos[
            (df_merge_reemplazos['difference'] >= -60) & (df_merge_reemplazos['difference'] <= 60)]

        # 8. because we didn´t match with the object information while looking for replacements , there could be posibility that
        # multiple new policies are found for one old or multiple old policies are found for one new policy. We have to find both cases.

        # 8.1 case where multiple old policies are found for one new policy
        df_reemplazos_multiple_1 = df_reemplazos_found.groupby(['poliza_id_y']).agg(
            {'poliza_id_x': 'count'}).reset_index().rename(columns={'poliza_id_x': 'count'})
        df_reemplazos_gt1 = df_reemplazos_multiple_1[df_reemplazos_multiple_1['count'] > 1]

        # 8.2 delete 'multiple old policies are found for one new policy' from df_reemplazos_found
        df_real_reemplazos = df_reemplazos_found[
            ~df_reemplazos_found['poliza_id_y'].isin(df_reemplazos_gt1.poliza_id_y)]

        # 8.3 case where multiple new policies are found for one old policy
        df_reemplazos_multiple_2 = df_real_reemplazos.groupby(['poliza_id_x']).agg(
            {'poliza_id_y': 'count'}).reset_index().rename(columns={'poliza_id_y': 'count'})
        df_reemplazos_gt1_2 = df_reemplazos_multiple_2[df_reemplazos_multiple_2['count'] > 1]

        # 8.4 delete 'multiple new policies are found for one old policy' from df_real_reemplazos
        df_real_reemplazos_2 = df_real_reemplazos[
            ~df_real_reemplazos['poliza_id_x'].isin(df_reemplazos_gt1_2.poliza_id_x)]

        # 9 pick only the required columns from the dataset ie old policy, new policy , start date of old policy
        df_real_reemplazos_3 = df_real_reemplazos_2[['poliza_id_x', 'poliza_fecha_inicio_x', 'poliza_id_y']]

        # 10. implement reeplacement

        # 10.1  take the original dataset and merge it with the final reemplazos dataset by the new policy_id. This gives us all information
        # about the new policy
        df_5_4 = pd.merge(left=df_5_3, right=df_real_reemplazos_3, left_on=['poliza_id'], right_on=['poliza_id_y'],
                          how='left')

        # 10.2 Change the start of the the new policy with the start date of the old policy
        df_5_4['poliza_fecha_inicio'] = np.where(df_5_4['poliza_fecha_inicio_x'].isnull(),
                                                 df_5_4['poliza_fecha_inicio'], df_5_4['poliza_fecha_inicio_x'])

        # 10.3 Delete the old policies
        df_5_5 = df_5_4[~df_5_4['poliza_id'].isin(df_real_reemplazos_3.poliza_id_x)]

        # df_5_5 will be your new cleaned "policy bottle"


        # 11.delete not required columns
        del df_5_5['poliza_id_x']
        #
        del df_5_5['poliza_fecha_inicio_x']
        #
        del df_5_5['poliza_id_y']

        # 12. change the dtype of poliza_fecha_inicio
        df_5_5['poliza_fecha_inicio'] = df_5_5.poliza_fecha_inicio.astype(int)

        return df_5_5

    def clean_reemplazos_hogar(df_5_5, df_object_hogar):
        # 1. put the policy data of inactive policies of Auto in df_A. This represents are old policies
        df_A_1 = df_5_5[(df_5_5['poliza_estado'] == 'A') & (df_5_5['poliza_agrup_prod_cod'].isin(['HOGAR']))][
            ['cod_global_unico', 'poliza_id', 'poliza_estado', 'poliza_fecha_cambio_estado', 'poliza_fecha_inicio',
             'poliza_fecha_emision']]

        # 2. put the policy data of active and inacitve policies of Auto in df_V_A. This represent  new policies
        df_V_A_1 = df_5_5[(df_5_5['poliza_estado'].isin(['V', 'A'])) & (df_5_5['poliza_agrup_prod_cod'].isin(['HOGAR']))][
            ['cod_global_unico', 'poliza_id', 'poliza_estado', 'poliza_fecha_cambio_estado', 'poliza_fecha_inicio',
             'poliza_fecha_emision']]

        # 3. pick only required fields from df_object_hogar
        df_object_hogar = df_object_hogar[['poliza_id', 'hogar_cp']]  # 594349

        # 5. merge the hogar object information with old policy dataset
        df_A_1 = pd.merge(left=df_A_1, right=df_object_hogar, left_on=['poliza_id'], right_on=['poliza_id'], how='left')

        # 6. merge the hogar object information with new policy dataset
        df_V_A_1 = pd.merge(left=df_V_A_1, right=df_object_hogar, left_on=['poliza_id'], right_on=['poliza_id'], how='left')

        # 7.merge the df_A and df_V_A based on client code and the product

        df_merge_reemplazos = pd.merge(left=df_A_1, right=df_V_A_1, left_on=['cod_global_unico'],
                                       right_on=['cod_global_unico'])
        df_merge_reemplazos = df_merge_reemplazos[
            (df_merge_reemplazos['poliza_id_x'] != df_merge_reemplazos['poliza_id_y'])]
        df_merge_reemplazos.loc[df_merge_reemplazos['poliza_fecha_cambio_estado_x'] == 0, 'poliza_fecha_cambio_estado_x'] = \
        df_merge_reemplazos['poliza_fecha_emision_x']

        df_merge_reemplazos = df_merge_reemplazos[
            (df_merge_reemplazos['hogar_cp_x'].notnull()) & (df_merge_reemplazos['hogar_cp_x'] != '?')
            & (df_merge_reemplazos['hogar_cp_y'].notnull()) & (df_merge_reemplazos['hogar_cp_y'] != '?')]

        df_merge_reemplazos = df_merge_reemplazos[
            df_merge_reemplazos['hogar_cp_x'] == df_merge_reemplazos['hogar_cp_y']]  # 407

        # 10. calculate the difference in the churn date of the old policy and start date of the new policy
        df_merge_reemplazos['poliza_fecha_cambio_estado_x'] = pd.to_datetime(
            df_merge_reemplazos['poliza_fecha_cambio_estado_x'], format='%Y%m%d')
        df_merge_reemplazos['poliza_fecha_inicio_y'] = pd.to_datetime(df_merge_reemplazos['poliza_fecha_inicio_y'],
                                                                      format='%Y%m%d')
        df_merge_reemplazos['difference'] = df_merge_reemplazos['poliza_fecha_inicio_y'].sub(
            df_merge_reemplazos['poliza_fecha_cambio_estado_x'], axis=0)
        df_merge_reemplazos['difference'] = df_merge_reemplazos['difference'] / np.timedelta64(1, 'D')

        # 11. reemplazos are those where the difference is between +- 60 days . Sometime it could be negative because they update the churn date of the
        # old policy later in time.
        df_reemplazos_found = df_merge_reemplazos[
            (df_merge_reemplazos['difference'] >= -60) & (df_merge_reemplazos['difference'] <= 60)]

        # 12.there could be posibility that multiple new policies are found for one old or
        # multiple old policies are found for one new policy. We have to find both cases.

        ##12.1 case where multiple old policies are found for one new policy
        df_reemplazos_multiple_1 = df_reemplazos_found.groupby(['poliza_id_y']).agg(
            {'poliza_id_x': 'count'}).reset_index().rename(columns={'poliza_id_x': 'count'})
        df_reemplazos_gt1 = df_reemplazos_multiple_1[df_reemplazos_multiple_1['count'] > 1]

        # 12.2 delete 'multiple old policies are found for one new policy' from df_reemplazos_found
        df_real_reemplazos = df_reemplazos_found[~df_reemplazos_found['poliza_id_y'].isin(df_reemplazos_gt1.poliza_id_y)]

        ##12.3 case where multiple new policies are found for one old policy
        df_reemplazos_multiple_2 = df_real_reemplazos.groupby(['poliza_id_x']).agg(
            {'poliza_id_y': 'count'}).reset_index().rename(columns={'poliza_id_y': 'count'})
        df_reemplazos_gt1_2 = df_reemplazos_multiple_2[df_reemplazos_multiple_2['count'] > 1]

        # 12.4 delete 'multiple new policies are found for one old policy' from df_real_reemplazos
        df_real_reemplazos_2 = df_real_reemplazos[~df_real_reemplazos['poliza_id_x'].isin(df_reemplazos_gt1_2.poliza_id_x)]

        # 13 pick only the required columns from the dataset ie old policy, new policy , start date of old policy
        df_real_reemplazos_3 = df_real_reemplazos_2[['poliza_id_x', 'poliza_fecha_inicio_x', 'poliza_id_y']]

        # 14 change the dtype of old policy start date
        df_real_reemplazos_3['poliza_fecha_inicio_x'] = df_real_reemplazos_3.poliza_fecha_inicio_x.astype(int)

        # 15.implement reeplacement

        # 15.1  take the original dataset and merge it with the final reemplazos dataset by the new policy_id. This gives us all information
        # about the new policy
        df_6 = pd.merge(left=df_5_5, right=df_real_reemplazos_3, left_on=['poliza_id'], right_on=['poliza_id_y'],
                        how='left')

        # 15.2 the start date of the policy is modified in such a way that it has int datatype

        df_6['poliza_fecha_inicio_x'] = df_6['poliza_fecha_inicio_x'].fillna(-1)
        df_6['poliza_fecha_inicio_x'] = df_6['poliza_fecha_inicio_x'].astype(int)
        df_6['poliza_fecha_inicio_x'] = df_6['poliza_fecha_inicio_x'].replace(-1, np.nan)

        # 15.3 Change the start of the the new policy with the start date of the old policy
        df_6['poliza_fecha_inicio'] = np.where(df_6['poliza_fecha_inicio_x'].isnull(), df_6['poliza_fecha_inicio'],
                                               df_6['poliza_fecha_inicio_x'])

        # 16. Delete the old policies
        df_6 = df_6[~df_6['poliza_id'].isin(df_real_reemplazos_3.poliza_id_x)]

        # df_6 will be your new cleaned "policy bottle"


        # 17. delete not required columns
        del df_6['poliza_id_x']
        del df_6['poliza_fecha_inicio_x']
        del df_6['poliza_id_y']

        # 18. change the dtype of poliza_fecha_inicio
        df_6['poliza_fecha_inicio'] = df_6.poliza_fecha_inicio.astype(int)

        # 19. delete customers for which more than one replacement is found. Because the data for these customers is not correct

        df_bad_data_client = df_6[df_6['poliza_id'].isin(df_reemplazos_gt1.poliza_id_y)]
        df_bad_data_client_2 = df_6[df_6['poliza_id'].isin(df_reemplazos_gt1_2.poliza_id_x)]
        df_6_1 = df_6[~df_6['cod_global_unico'].isin(df_bad_data_client.cod_global_unico)]
        df_6_2 = df_6_1[~df_6_1['cod_global_unico'].isin(df_bad_data_client_2.cod_global_unico)]

        return df_6_2

    def clean_bottle(bottle_tenencia2):
        '''
             this function clean the tenency bottle by identifying "reemplazos" in Hogar and Others productos
             Spanish: Esta función limpia la botella tenencia identificando reemplazos en los productos hogar y otros.

            :return: clean tenency bottle with not replacement
        '''
        bottle_tenencia3_hogar = bottle_tenencia2[bottle_tenencia2['poliza_agrup_prod_cod'] == 'HOGAR']
        bottle_tenencia3_auto = bottle_tenencia2[bottle_tenencia2['poliza_agrup_prod_cod'] == 'AUTOS']
        bottle_tenencia3_otros = bottle_tenencia2[~bottle_tenencia2['poliza_agrup_prod_cod'].isin(['HOGAR', 'AUTOS'])]
        print("Hogar ", bottle_tenencia3_hogar.shape)
        print("Auto ", bottle_tenencia3_auto.shape)
        print("Otros ", bottle_tenencia3_auto.shape)

        bottle_tenencia3_hogar = Home.clean_reemplazos_other(bottle_tenencia3_hogar)
        bottle_tenencia3_otros = Home.clean_reemplazos_other(bottle_tenencia3_otros)

        tenencia_clean = pd.concat([bottle_tenencia3_hogar, bottle_tenencia3_otros, bottle_tenencia3_auto], axis=0)

        print("Size of tenencia es", tenencia_clean.shape)

        print(tenencia_clean.head())
        print("Termino de limpiar la botella tenencia")

        return tenencia_clean


