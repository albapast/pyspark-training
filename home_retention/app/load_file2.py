
import pandas as pd
import numpy as np



def load_file_AUTO_1_4_12(path_file, file_name):
    header = ['p_id',
            'p_duracion',
            'p_estado',
            'p_version',
            'p_entidad_legal',
            'p_producto_tecnico',
            'p_producto_comercial',
            'p_fch_ultimo_reemplazo',
            'p_motivo_cancelacion_cod',
            'p_motivo_cancelacion_desc',
            'p_fch_efecto_nat',
            'p_fch_efecto_mov',
            'p_fch_emision_anulacion',
            'p_fch_cambio_estado',
            'p_fch_cartera',
            'p_fch_inicio',
            'p_fch_emision',
            'p_fch_vencimiento',
            'p_estructura_cod',
            'p_estructura_desc',
            'p_canal',
            'p_sub_canal',
            'p_negocio_cod',
            'p_negocio_desc',
            'p_negocio_padre_cod',
            'p_negocio_padre_desc',
            'p_negocio_abuelo_cod',
            'p_negocio_abuelo_desc',
            'p_negocio_bisabuelo_cod',
            'p_negocio_bisabuelo_desc',
            'p_segmento_negocio',
            'p_med',
            'audit_p_versiones',
            'audit_p_estados',
            'audit_p_tiene_ultima_situacion',
            'audit_p_tiene_anulacion',
            'audit_fch_cierre',
            'p_forma_pago_cod',
            'p_forma_pago_desc',
            'p_modalidad_garantias',
            'p_modalidad_pack',
            'p_modalidad_danos',
            'cl_p_auto_duracion_maxima',
            'cl_nif',
            'cl_codfiliacion',
            'cl_fenaccon',
            'cl_fchini_zurich',
            'cl_feulaccl',
            'cl_incl',
            'cl_cod_postal',
            'cl_sexo_desc',
            'cl_presencia_tabla_morosidad',
            'cl_robinson',
            'cl_provincia',
            'cl_num_devoluciones',
            'cl_tiene_email',
            'cl_ccaa',
            'cl_tiene_fijo',
            'cl_tiene_movil',
            'cl_clase_persona_cod',
            'cl_tcnacper',
            'cl_tcnacper_desc',
            'cl_tcnacres',
            'cl_tcnacres_desc',
            'vehi_cod',
            'vehi_uso_cod',
            'vehi_uso_desc',
            'vehi_matricula',
            'vehi_valor',
            'vehi_clase_cod',
            'vehi_clase_descripcion',
            'vehi_clase_agrupacion_descripcion',
            'vehi_potencia',
            'vehi_marca_cod',
            'vehi_marca_desc',
            'vehi_modelo_cod',
            'vehi_modelo_desc',
            'vehi_bonus_cod',
            'vehi_bonus_desc',
            'vehi_fenacco_conductor1',
            'vehi_fenacco_conductor2',
            'vehi_fepecon_conductor1',
            'vehi_fepecon_conductor2',
            'vehi_fch_mat',
            'vehi_fch_adquicision',
            'vehi_categoria',
            'p_cambio_tomador',
            'p_movimiento_cod',
            'p_movimiento_desc',
            'p_motivo_movimiento_cod',
            'p_motivo_movimiento_desc',
            'med_Direccion_territorial_cod',
            'med_Direccion_territorial_desc',
            'med_gerencia_cod',
            'med_gerencia_desc',
            'med_responsable_comercial_cod',
            'med_dn',
            'med_nombre',
            'med_tipo',
            'med_clase',
            'med_TCPEFIJU',
            'med_segmento_cod',
            'med_segmento_desc',
            'med_cp',
            'med_provincia',
            'med_tctiesin',
            'med_tctiesin_desc',
            'med_oficina_cod',
            'med_oficina_desc',
            'med_condiciones_especiales',
            'med_condiciones_especiales_desc',
            'med_indice_abandono',
            'p_med_numpol_vigor_auto',
            'p_med_numpol_anuladas_auto',
            'p_med_numpol_vigor',
            'p_med_numpol_anuladas',
            'cl_direccion',
            'p_cambio_med',
            'p_cambio_garantias',
            'cl_cod_tipodoc',
            'p_cambio_provincia',
            'p_med_numpol_anuladas_saneamiento_auto',
            'p_med_numpol_anuladas_saneamiento',
            'med_id_fiscal',
            'vehi_tipo_combustible']

    ruta = path_file + file_name
    file = pd.read_csv(ruta,sep=',', encoding="latin1", dtype={"p_id": object, "cl_nif": object,
                                                       "cl_codfiliacion": object, "vehi_uso_cod": object, "vehi_valor": object, "vehiculo_potencia": object }, index_col=None)

    file['p_fch_cartera2'] = file['p_fch_vencimiento'] // 100  # Aqui tengo el AnoMEs de la cartera
    file['p_fch_cartera3'] = file['p_fch_cartera2'] % 100  # Aqui tengo el MES de la cartera
    file['p_fch_emision_anulacion2'] = file['p_fch_emision_anulacion'][0:6]  # Aqui tengo el MES de la cartera

    file['p_fch_inicio2'] = file['p_fch_inicio'] // 100  # Aqui AnoMes Inicio
    file['p_fch_cambio_estado2'] = file['p_fch_cambio_estado'] // 100  # Aqui AnoMes Cambio de Estado
    file['p_fch_efecto_nat2'] = file['p_fch_efecto_nat'] // 100  # Aqui AnoMes Fecha efecto natural
    file['cl_fenaccon2'] = file['cl_fenaccon'] // 100  # Aqui AnoMes Fecha nacimiento

    print("Aqui leyo la botella: " + file_name)

    fn = file_name[14:22]
    return file, fn

def load_file_AUTO_1_6(path_file, file_name):
    header = ['p_id',
            'p_duracion',
            'p_estado',
            'p_version',
            'p_entidad_legal',
            'p_producto_tecnico',
            'p_producto_comercial',
            'p_fch_ultimo_reemplazo',
            'p_motivo_cancelacion_cod',
            'p_motivo_cancelacion_desc',
            'p_fch_efecto_nat',
            'p_fch_efecto_mov',
            'p_fch_emision_anulacion',
            'p_fch_cambio_estado',
            'p_fch_cartera',
            'p_fch_inicio',
            'p_fch_emision',
            'p_fch_vencimiento',
            'p_estructura_cod',
            'p_estructura_desc',
            'p_canal',
            'p_sub_canal',
            'p_negocio_cod',
            'p_negocio_desc',
            'p_negocio_padre_cod',
            'p_negocio_padre_desc',
            'p_negocio_abuelo_cod',
            'p_negocio_abuelo_desc',
            'p_negocio_bisabuelo_cod',
            'p_negocio_bisabuelo_desc',
            'p_segmento_negocio',
            'p_med',
            'audit_p_versiones',
            'audit_p_estados',
            'audit_p_tiene_ultima_situacion',
            'audit_p_tiene_anulacion',
            'audit_fch_cierre',
            'p_forma_pago_cod',
            'p_forma_pago_desc',
            'p_modalidad_garantias',
            'p_modalidad_pack',
            'p_modalidad_danos',
            'cl_p_auto_duracion_maxima',
            'cl_nif',
            'cl_codfiliacion',
            'cl_fenaccon',
            'cl_fchini_zurich',
            'cl_feulaccl',
            'cl_incl',
            'cl_cod_postal',
            'cl_sexo_desc',
            'cl_presencia_tabla_morosidad',
            'cl_robinson',
            'cl_provincia',
            'cl_num_devoluciones',
            'cl_tiene_email',
            'cl_ccaa',
            'cl_tiene_fijo',
            'cl_tiene_movil',
            'cl_clase_persona_cod',
            'cl_tcnacper',
            'cl_tcnacper_desc',
            'cl_tcnacres',
            'cl_tcnacres_desc',
            'vehi_cod',
            'vehi_uso_cod',
            'vehi_uso_desc',
            'vehi_matricula',
            'vehi_valor',
            'vehi_clase_cod',
            'vehi_clase_descripcion',
            'vehi_clase_agrupacion_descripcion',
            'vehi_potencia',
            'vehi_marca_cod',
            'vehi_marca_desc',
            'vehi_modelo_cod',
            'vehi_modelo_desc',
            'vehi_bonus_cod',
            'vehi_bonus_desc',
            'vehi_fenacco_conductor1',
            'vehi_fenacco_conductor2',
            'vehi_fepecon_conductor1',
            'vehi_fepecon_conductor2',
            'vehi_fch_mat',
            'vehi_fch_adquicision',
            'vehi_categoria',
            'p_cambio_tomador',
            'p_movimiento_cod',
            'p_movimiento_desc',
            'p_motivo_movimiento_cod',
            'p_motivo_movimiento_desc',
            'med_Direccion_territorial_cod',
            'med_Direccion_territorial_desc',
            'med_gerencia_cod',
            'med_gerencia_desc',
            'med_responsable_comercial_cod',
            'med_dn',
            'med_nombre',
            'med_tipo',
            'med_clase',
            'med_TCPEFIJU',
            'med_segmento_cod',
            'med_segmento_desc',
            'med_cp',
            'med_provincia',
            'med_tctiesin',
            'med_tctiesin_desc',
            'med_oficina_cod',
            'med_oficina_desc',
            'med_condiciones_especiales',
            'med_condiciones_especiales_desc',
            'med_indice_abandono',
            'p_med_numpol_vigor_auto',
            'p_med_numpol_anuladas_auto',
            'p_med_numpol_vigor',
            'p_med_numpol_anuladas',
            'cl_direccion',
            'p_cambio_med',
            'p_cambio_garantias',
            'cl_cod_tipodoc',
            'p_cambio_provincia',
            'p_med_numpol_anuladas_saneamiento_auto',
            'p_med_numpol_anuladas_saneamiento',
            'med_id_fiscal',
            'vehi_tipo_combustible']

    ruta = path_file + file_name
    file = pd.read_csv(ruta, sep=',', names=header, dtype={"p_id": object, "cl_nif": object, "cl_codfiliacion": object, "vehi_uso_cod": object, "vehi_valor": object, "vehiculo_potencia": object}, index_col=None)
    # Selecciono las variables que tienen informacion
    file = file.loc[:, ['p_id',
                        'p_duracion',
                        'p_estado',
                        'p_version',
                        'p_entidad_legal',
                        'p_producto_tecnico',
                        'p_producto_comercial',
                        'p_fch_ultimo_reemplazo',
                        'p_motivo_cancelacion_cod',
                        'p_motivo_cancelacion_desc',
                        'p_fch_efecto_nat',
                        'p_fch_efecto_mov',
                        'p_fch_emision_anulacion',
                        'p_fch_cambio_estado',
                        'p_fch_cartera',
                        'p_fch_inicio',
                        'p_fch_emision',
                        'p_fch_vencimiento',
                        'p_estructura_cod',
                        'p_estructura_desc',
                        'p_canal',
                        'p_neg_cod',
                        'p_neg_desc',
                        'p_neg_padre_cod',
                        'p_neg_padre_desc',
                        'p_med',
                        'audit_fch_cierre',
                        'p_forma_pago_cod',
                        'p_forma_pago_desc',
                        'p_modalidad_garantias',
                        'p_modalidad_pack',
                        'p_modalidad_danos',
                        'cl_p_auto_duracion_maxima',
                        'cl_nif',
                        'cl_codfiliacion',
                        'cl_fenaccon',
                        'cl_fchini_zurich',
                        'cl_feulaccl',
                        'cl_incl',
                        'cl_cod_postal',
                        'cl_sexo_desc',
                        'cl_presencia_tabla_morosidad',
                        'cl_robinson',
                        'cl_provincia',
                        'cl_tiene_email',
                        'cl_ccaa',
                        'cl_tiene_fijo',
                        'cl_tiene_movil',
                        'cl_clase_persona_cod',
                        'cl_tcnacper',
                        'cl_tcnacper_desc',
                        'cl_tcnacres',
                        'cl_tcnacres_desc',
                        'vehi_cod',
                        'vehi_uso_cod',
                        'vehi_uso_desc',
                        'vehi_matricula',
                        'vehi_valor',
                        'vehi_clase_cod',
                        'vehi_clase_descripcion',
                        'vehi_clase_agrupacion_descripcion',
                        'vehi_potencia',
                        'vehi_marca_cod',
                        'vehi_marca_desc',
                        'vehi_modelo_cod',
                        'vehi_modelo_desc',
                        'vehi_bonus_cod',
                        'vehi_bonus_desc',
                        'vehi_fenacco_conductor1',
                        'vehi_fenacco_conductor2',
                        'vehi_fepecon_conductor1',
                        'vehi_fepecon_conductor2',
                        'vehi_fch_mat',
                        'vehi_categoria',
                        'p_cambio_tomador',
                        'p_movimiento_cod',
                        'p_movimiento_desc',
                        'p_motivo_movimiento_cod',
                        'p_motivo_movimiento_desc',
                        'med_responsable_comercial_cod',
                        'med_dn',
                        'med_nombre',
                        'med_clase',
                        'med_TCPEFIJU',
                        'med_segmento_cod',
                        'med_segmento_desc',
                        'med_cp',
                        'med_provincia',
                        'med_tctiesin',
                        'med_tctiesin_desc',
                        'med_oficina_cod',
                        'med_oficina_desc',
                        'p_med_numpol_vigor_auto',
                        'p_med_numpol_anuladas_auto',
                        'p_med_numpol_vigor',
                        'p_med_numpol_anuladas',
                        'cl_direccion',
                        'p_cambio_med',
                        'p_cambio_garantias',
                        'cl_cod_tipodoc',
                        'p_cambio_provincia',
                        'p_med_numpol_anuladas_saneamiento_auto',
                        'p_med_numpol_anuladas_saneamiento',
                        'med_id_fiscal',
                        'vehi_tipo_combustible']]

    file['p_fch_cartera2'] = file['p_fch_vencimiento'] // 100  # Aqui tengo el AnoMEs de la cartera
    file['p_fch_cartera3'] = file['p_fch_cartera2'] % 100  # Aqui tengo el MES de la cartera
    file['p_fch_emision_anulacion2'] = file['p_fch_emision_anulacion'][0:6]  # Aqui tengo el MES de la cartera

    file['p_fch_inicio2'] = file['p_fch_inicio'] // 100  # Aqui AnoMes Inicio
    file['p_fch_cambio_estado2'] = file['p_fch_cambio_estado'] // 100  # Aqui AnoMes Cambio de Estado
    file['p_fch_efecto_nat2'] = file['p_fch_efecto_nat'] // 100  # Aqui AnoMes Fecha efecto natural
    file['cl_fenaccon2'] = file['cl_fenaccon'] // 100  # Aqui AnoMes Fecha nacimiento

    print("Aqui leyo la botella: " + file_name)

    fn = file_name[14:22]
    return file, fn


def load_file_AUTO_1_7(path_file, file_name):
    header = ['p_id',
                'p_duracion',
                'p_estado',
                'p_version',
                'p_entidad_legal',
                'p_producto_tecnico',
                'p_producto_comercial',
                'p_fch_ultimo_reemplazo',
                'p_motivo_cancelacion_cod',
                'p_motivo_cancelacion_desc',
                'p_fch_efecto_nat',
                'p_fch_efecto_mov',
                'p_fch_emision_anulacion',
                'p_fch_cambio_estado',
                'p_fch_cartera',
                'p_fch_inicio',
                'p_fch_emision',
                'p_fch_vencimiento_natural',
                'p_fch_vencimiento_movimiento',
                'p_estructura_cod',
                'p_estructura_desc',
                'p_canal',
                'p_sub_canal',
                'p_neg_cod',
                'p_neg_desc',
                'p_neg_padre_cod',
                'p_neg_padre_desc',
                'p_neg_abuelo_cod',
                'p_neg_abuelo_desc',
                'p_neg_bisabuelo_cod',
                'p_neg_bisabuelo_desc',
                'p_segmento_neg',
                'p_med',
                'audit_p_versiones',
                'audit_p_estados',
                'audit_p_tiene_ultima_situacion',
                'audit_p_tiene_anulacion',
                'audit_fch_cierre',
                'p_forma_pago_cod',
                'p_forma_pago_desc',
                'p_modalidad_garantias',
                'p_modalidad_pack',
                'p_modalidad_danos',
                'cl_p_auto_duracion_maxima',
                'cl_nif',
                'cl_codfiliacion',
                'cl_fenaccon',
                'cl_fchini_zurich',
                'cl_feulaccl',
                'cl_incl',
                'cl_cod_postal',
                'cl_sexo_desc',
                'cl_presencia_tabla_morosidad',
                'cl_robinson',
                'cl_provincia',
                'cl_num_devoluciones',
                'cl_tiene_email',
                'cl_ccaa',
                'cl_tiene_fijo',
                'cl_tiene_movil',
                'cl_clase_persona_cod',
                'cl_tcnacper',
                'cl_tcnacper_desc',
                'cl_tcnacres',
                'cl_tcnacres_desc',
                'vehi_cod',
                'vehi_uso_cod',
                'vehi_uso_desc',
                'vehi_matricula',
                'vehi_valor',
                'vehi_clase_cod',
                'vehi_clase_descripcion',
                'vehi_clase_agrupacion_descripcion',
                'vehi_potencia',
                'vehi_marca_cod',
                'vehi_marca_desc',
                'vehi_modelo_cod',
                'vehi_modelo_desc',
                'vehi_bonus_cod',
                'vehi_bonus_desc',
                'vehi_fenacco_conductor1',
                'vehi_fenacco_conductor2',
                'vehi_fepecon_conductor1',
                'vehi_fepecon_conductor2',
                'vehi_fch_mat',
                'vehi_fch_adquicision',
                'vehi_categoria',
                'p_cambio_tomador',
                'p_movimiento_cod',
                'p_movimiento_desc',
                'p_motivo_movimiento_cod',
                'p_motivo_movimiento_desc',
                'med_Direccion_territorial_cod',
                'med_Direccion_territorial_desc',
                'med_gerencia_cod',
                'med_gerencia_desc',
                'med_responsable_comercial_cod',
                'med_dn',
                'med_nombre',
                'med_tipo',
                'med_clase',
                'med_TCPEFIJU',
                'med_segmento_cod',
                'med_segmento_desc',
                'med_cp',
                'med_provincia',
                'med_tctiesin',
                'med_tctiesin_desc',
                'med_oficina_cod',
                'med_oficina_desc',
                'med_condiciones_especiales',
                'med_condiciones_especiales_desc',
                'med_indice_abandono',
                'p_med_numpol_vigor_auto',
                'p_med_numpol_anuladas_auto',
                'p_med_numpol_vigor',
                'p_med_numpol_anuladas',
                'cl_direccion',
                'p_cambio_med',
                'p_cambio_garantias',
                'cl_cod_tipodoc',
                'p_cambio_provincia',
                'p_med_numpol_anuladas_saneamiento_auto',
                'p_med_numpol_anuladas_saneamiento',
                'med_id_fiscal',
                'vehi_tipo_combustible',
                'pMigracion']

    ruta = path_file + file_name
    file = pd.read_csv(ruta, sep=',', names=header, dtype={"p_id": object,
                                                           "cl_nif": object,
                                                           "p_neg_cod": object,
                                                           "p_neg_padre_cod": object,
                                                           "cl_cod_postal": object,
                                                           "vehi_potencia": object,
                                                           "vehi_marca_cod": object,
                                                           "vehi_bonus_desc": object,
                                                           "vehi_modelo_cod": object,
                                                           "vehi_bonus_cod": object,
                                                           "cl_codfiliacion": object,
                                                           "vehi_uso_cod": object,
                                                           "vehi_valor": object,
                                                           "vehi_fenacco_conductor1": object,
                                                           "vehi_fenacco_conductor2": object,
                                                           "vehi_fepecon_conductor1": object,
                                                           "vehi_fepecon_conductor2": object,
                                                           "p_movimiento_cod": object,
                                                           "vehi_fch_mat": object,
                                                           "med_responsable_comercial_cod": object,
                                                           "med_cp": object
                                                           }, index_col=None)
    # Selecciono las variables que tienen informacion
    file = file.loc[:, ['p_id',
                    'p_duracion',
                    'p_estado',
                    'p_version',
                    'p_entidad_legal',
                    'p_producto_tecnico',
                    'p_producto_comercial',
                    'p_fch_ultimo_reemplazo',
                    'p_motivo_cancelacion_cod',
                    'p_motivo_cancelacion_desc',
                    'p_fch_efecto_nat',
                    'p_fch_efecto_mov',
                    'p_fch_emision_anulacion',
                    'p_fch_cambio_estado',
                    'p_fch_cartera',
                    'p_fch_inicio',
                    'p_fch_emision',
                    'p_fch_vencimiento_natural',
                    'p_fch_vencimiento_movimiento',
                    'p_estructura_cod',
                    'p_estructura_desc',
                    'p_canal',
                    'p_neg_cod',
                    'p_neg_desc',
                    'p_neg_padre_cod',
                    'p_neg_padre_desc',
                    'p_med',
                    'audit_p_versiones',
                    'audit_p_estados',
                    'audit_p_tiene_ultima_situacion',
                    'audit_p_tiene_anulacion',
                    'audit_fch_cierre',
                    'p_forma_pago_cod',
                    'p_forma_pago_desc',
                    'p_modalidad_garantias',
                    'p_modalidad_pack',
                    'p_modalidad_danos',
                    'cl_p_auto_duracion_maxima',
                    'cl_nif',
                    'cl_codfiliacion',
                    'cl_fenaccon',
                    'cl_fchini_zurich',
                    'cl_feulaccl',
                    'cl_incl',
                    'cl_cod_postal',
                    'cl_sexo_desc',
                    'cl_presencia_tabla_morosidad',
                    'cl_robinson',
                    'cl_provincia',
                    'cl_tiene_email',
                    'cl_ccaa',
                    'cl_tiene_fijo',
                    'cl_tiene_movil',
                    'cl_clase_persona_cod',
                    'cl_tcnacper',
                    'cl_tcnacper_desc',
                    'cl_tcnacres',
                    'cl_tcnacres_desc',
                    'vehi_cod',
                    'vehi_uso_cod',
                    'vehi_uso_desc',
                    'vehi_matricula',
                    'vehi_valor',
                    'vehi_clase_cod',
                    'vehi_clase_descripcion',
                    'vehi_clase_agrupacion_descripcion',
                    'vehi_potencia',
                    'vehi_marca_cod',
                    'vehi_marca_desc',
                    'vehi_modelo_cod',
                    'vehi_modelo_desc',
                    'vehi_bonus_cod',
                    'vehi_bonus_desc',
                    'vehi_fenacco_conductor1',
                    'vehi_fenacco_conductor2',
                    'vehi_fepecon_conductor1',
                    'vehi_fepecon_conductor2',
                    'vehi_fch_mat',
                    'vehi_categoria',
                    'p_cambio_tomador',
                    'p_movimiento_cod',
                    'p_movimiento_desc',
                    'p_motivo_movimiento_cod',
                    'p_motivo_movimiento_desc',
                    'med_responsable_comercial_cod',
                    'med_dn',
                    'med_nombre',
                    'med_clase',
                    'med_TCPEFIJU',
                    'med_segmento_cod',
                    'med_segmento_desc',
                    'med_cp',
                    'med_provincia',
                    'med_tctiesin',
                    'med_tctiesin_desc',
                    'med_oficina_cod',
                    'med_oficina_desc',
                    'p_med_numpol_vigor_auto',
                    'p_med_numpol_anuladas_auto',
                    'p_med_numpol_vigor',
                    'p_med_numpol_anuladas',
                    'cl_direccion',
                    'p_cambio_med',
                    'p_cambio_garantias',
                    'cl_cod_tipodoc',
                    'p_cambio_provincia',
                    'p_med_numpol_anuladas_saneamiento_auto',
                    'p_med_numpol_anuladas_saneamiento',
                    'med_id_fiscal',
                    'vehi_tipo_combustible',
                    'pMigracion']]

    file['p_fch_cartera2'] = file['p_fch_vencimiento_natural'] // 100  # Aqui tengo el AnoMEs de la cartera
    file['p_fch_cartera3'] = file['p_fch_cartera2'] % 100  # Aqui tengo el MES de la cartera
    file['p_fch_emision_anulacion2'] = file['p_fch_emision_anulacion'][0:6]  # Aqui tengo el MES de la cartera

    file['p_fch_inicio2'] = file['p_fch_inicio'] // 100  # Aqui AnoMes Inicio
    file['p_fch_cambio_estado2'] = file['p_fch_cambio_estado'] // 100  # Aqui AnoMes Cambio de Estado
    file['p_fch_efecto_nat2'] = file['p_fch_efecto_nat'] // 100  # Aqui AnoMes Fecha efecto natural
    file['p_fch_efecto_mov2'] = file['p_fch_efecto_mov'] // 100  # Aqui AnoMes Fecha efecto natural
    file['cl_fenaccon2'] = file['cl_fenaccon'] // 100  # Aqui AnoMes Fecha nacimiento

    print("Aqui leyo la botella: " + file_name)

    fn = file_name[14:22]
    return file, fn


def load_file_AUTO_1_72(path_file, file_name):
    header = ['poliza_id',
              'p_duracion',
              'poliza_estado',
              'p_version',
              'p_entidad_legal',
              'p_producto_tecnico',
              'p_producto_comercial',
              'p_fch_ultimo_reemplazo',
              'p_motivo_cancelacion_cod',
              'poliza_motivo_cancelacion_desc',
              'p_fch_efecto_nat',
              'p_fch_efecto_mov',
              'p_fch_emision_anulacion',
              'p_fch_cambio_estado',
              'p_fch_cartera',
              'p_fch_inicio',
              'p_fch_emision',
              'poliza_fecha_vencimiento_natural',
              'p_fch_vencimiento_movimiento',
              'p_estructura_cod',
              'p_estructura_desc',
              'p_canal',
              'p_sub_canal',
              'p_neg_cod',
              'p_neg_desc',
              'p_neg_padre_cod',
              'p_neg_padre_desc',
              'p_neg_abuelo_cod',
              'p_neg_abuelo_desc',
              'p_neg_bisabuelo_cod',
              'p_neg_bisabuelo_desc',
              'p_segmento_neg',
              'p_med',
              'audit_p_versiones',
              'audit_p_estados',
              'audit_p_tiene_ultima_situacion',
              'audit_p_tiene_anulacion',
              'audit_fch_cierre',
              'p_forma_pago_cod',
              'p_forma_pago_desc',
              'p_modalidad_garantias',
              'p_modalidad_pack',
              'p_modalidad_danos',
              'cl_p_auto_duracion_maxima',
              'cl_nif',
              'cl_codfiliacion',
              'cl_fenaccon',
              'cl_fchini_zurich',
              'cl_feulaccl',
              'cl_incl',
              'cl_cod_postal',
              'cl_sexo_desc',
              'cl_presencia_tabla_morosidad',
              'cl_robinson',
              'cl_provincia',
              'cl_num_devoluciones',
              'cl_tiene_email',
              'cl_ccaa',
              'cl_tiene_fijo',
              'cl_tiene_movil',
              'cl_clase_persona_cod',
              'cl_tcnacper',
              'cl_tcnacper_desc',
              'cl_tcnacres',
              'cl_tcnacres_desc',
              'vehi_cod',
              'vehi_uso_cod',
              'vehi_uso_desc',
              'vehi_matricula',
              'vehi_valor',
              'vehi_clase_cod',
              'vehi_clase_descripcion',
              'vehi_clase_agrupacion_descripcion',
              'vehi_potencia',
              'vehi_marca_cod',
              'vehi_marca_desc',
              'vehi_modelo_cod',
              'vehi_modelo_desc',
              'vehi_bonus_cod',
              'vehi_bonus_desc',
              'vehi_fenacco_conductor1',
              'vehi_fenacco_conductor2',
              'vehi_fepecon_conductor1',
              'vehi_fepecon_conductor2',
              'vehi_fch_mat',
              'vehi_fch_adquicision',
              'vehi_categoria',
              'p_cambio_tomador',
              'p_movimiento_cod',
              'p_movimiento_desc',
              'p_motivo_movimiento_cod',
              'p_motivo_movimiento_desc',
              'med_Direccion_territorial_cod',
              'med_Direccion_territorial_desc',
              'med_gerencia_cod',
              'med_gerencia_desc',
              'med_responsable_comercial_cod',
              'med_dn',
              'med_nombre',
              'med_tipo',
              'med_clase',
              'med_TCPEFIJU',
              'med_segmento_cod',
              'med_segmento_desc',
              'med_cp',
              'med_provincia',
              'med_tctiesin',
              'med_tctiesin_desc',
              'med_oficina_cod',
              'med_oficina_desc',
              'med_condiciones_especiales',
              'med_condiciones_especiales_desc',
              'med_indice_abandono',
              'p_med_numpol_vigor_auto',
              'p_med_numpol_anuladas_auto',
              'p_med_numpol_vigor',
              'p_med_numpol_anuladas',
              'cl_direccion',
              'p_cambio_med',
              'p_cambio_garantias',
              'cl_cod_tipodoc',
              'p_cambio_provincia',
              'p_med_numpol_anuladas_saneamiento_auto',
              'p_med_numpol_anuladas_saneamiento',
              'med_id_fiscal',
              'vehi_tipo_combustible',
              'pMigracion']

    ruta = path_file + file_name
    file = pd.read_csv(ruta, sep=',', names=header, dtype={"poliza_id": object,
                                                           "cl_nif": object,
                                                           "p_neg_cod": object,
                                                           "p_neg_padre_cod": object,
                                                           "cl_cod_postal": object,
                                                           "vehi_potencia": object,
                                                           "vehi_marca_cod": object,
                                                           "vehi_bonus_desc": object,
                                                           "vehi_modelo_cod": object,
                                                           "vehi_bonus_cod": object,
                                                           "cl_codfiliacion": object,
                                                           "vehi_uso_cod": object,
                                                           "vehi_valor": object,
                                                           "vehi_fenacco_conductor1": object,
                                                           "vehi_fenacco_conductor2": object,
                                                           "vehi_fepecon_conductor1": object,
                                                           "vehi_fepecon_conductor2": object,
                                                           "p_movimiento_cod": object,
                                                           "vehi_fch_mat": object,
                                                           "med_responsable_comercial_cod": object,
                                                           "med_cp": object
                                                           }, index_col=None)

    file['poliza_fecha_cartera2'] = file[
                                        'poliza_fecha_vencimiento_natural'] // 100  # Aqui tengo el AnoMEs de la cartera

    print("Aqui leyo la botella: " + file_name)

    fn = file_name[16:22]
    return file, fn


def load_file_AUTO_matrix(path_file, file_name):

    ruta = path_file + file_name
    file = pd.read_csv(ruta, sep=',', dtype={"poliza_id": object,
                                              "cliente_nif": object,
                                              "poliza_negocio_codigo": object,
                                              "poliza_negocio_padre_codigo": object,
                                              "cliente_codigo_postal": object,
                                              "vehiculo_potencia": object,
                                              "vehiculo_marca_codigo": object,
                                              "vehiculo_bonus_desc": object,
                                              "vehiculo_modelo_codigo": object,
                                              "vehiculo_bonus_codigo": object,
                                              "cliente_codfiliacion": object,
                                              "vehiculo_uso_codigo": object,
                                              "vehiculo_valor": object,
                                              "vehiculo_fenacco_conductor1": object,
                                              "vehiculo_fenacco_conductor2": object,
                                              "vehiculo_fepecon_conductor1": object,
                                              "vehiculo_fepecon_conductor2": object,
                                              "poliza_movimiento_cod": object,
                                              "vehiculo_fecha_mat": object,
                                              "mediador_responsable_comercial_codigo": object,
                                              "mediador_cp": object
                                              } , index_col=None)
    # Selecciono las variables que tienen informacion

    '''
    file['p_fch_cartera2'] = file['p_fch_vencimiento_natural'] // 100  # Aqui tengo el AnoMEs de la cartera
    file['p_fch_cartera3'] = file['p_fch_cartera2'] % 100  # Aqui tengo el MES de la cartera
    file['p_fch_emision_anulacion2'] = file['p_fch_emision_anulacion'][0:6]  # Aqui tengo el MES de la cartera

    file['p_fch_inicio2'] = file['p_fch_inicio'] // 100  # Aqui AnoMes Inicio
    file['p_fch_cambio_estado2'] = file['p_fch_cambio_estado'] // 100  # Aqui AnoMes Cambio de Estado
    file['p_fch_efecto_nat2'] = file['p_fch_efecto_nat'] // 100  # Aqui AnoMes Fecha efecto natural
    file['p_fch_efecto_mov2'] = file['p_fch_efecto_mov'] // 100  # Aqui AnoMes Fecha efecto natural
    file['cl_fenaccon2'] = file['cl_fenaccon'] // 100  # Aqui AnoMes Fecha nacimiento

    print("Aqui leyo la botella: " + file_name)
    '''

    fn = file_name[14:22]
    return file, fn


def load_file_auto_matrix2(path_file, file_name):

    ruta = path_file + file_name
    file = pd.read_csv(ruta, sep=',', dtype={"poliza_id": object,
                                              "cliente_nif": object,
                                              "poliza_negocio_codigo": object,
                                              "poliza_negocio_padre_codigo": object,
                                              "cliente_codigo_postal": object,
                                              "vehiculo_potencia": object,
                                              "vehiculo_marca_codigo": object,
                                              "vehiculo_bonus_desc": object,
                                              "vehiculo_modelo_codigo": object,
                                              "vehiculo_bonus_codigo": object,
                                              "cliente_codfiliacion": object,
                                              "vehiculo_uso_codigo": object,
                                              "vehiculo_valor": object,
                                              "vehiculo_fenacco_conductor1": object,
                                              "vehiculo_fenacco_conductor2": object,
                                              "vehiculo_fepecon_conductor1": object,
                                              "vehiculo_fepecon_conductor2": object,
                                              "poliza_movimiento_cod": object,
                                              "vehiculo_fecha_mat": object,
                                              "mediador_responsable_comercial_codigo": object,
                                              "mediador_cp": object
                                              }, index_col=None)

    # Selecciono las variables que tienen informacion
    file['poliza_fecha_cartera2'] = file['poliza_fecha_vencimiento_natural'] // 100  # Aqui tengo el AnoMEs de la cartera

    '''
    file['p_fch_cartera3'] = file['p_fch_cartera2'] % 100  # Aqui tengo el MES de la cartera
    file['p_fch_emision_anulacion2'] = file['p_fch_emision_anulacion'][0:6]  # Aqui tengo el MES de la cartera

    file['p_fch_inicio2'] = file['p_fch_inicio'] // 100  # Aqui AnoMes Inicio
    file['p_fch_cambio_estado2'] = file['p_fch_cambio_estado'] // 100  # Aqui AnoMes Cambio de Estado
    file['p_fch_efecto_nat2'] = file['p_fch_efecto_nat'] // 100  # Aqui AnoMes Fecha efecto natural
    file['p_fch_efecto_mov2'] = file['p_fch_efecto_mov'] // 100  # Aqui AnoMes Fecha efecto natural
    file['cl_fenaccon2'] = file['cl_fenaccon'] // 100  # Aqui AnoMes Fecha nacimiento

    print("Aqui leyo la botella: " + file_name)
    '''

    fn = file_name[10:16]
    return file, fn


def load_file_HOGAR_matrix(path_file, file_name):

    path = path_file + file_name
    print(path)
    file = pd.read_csv(path, sep=',', dtype={"poliza_id": object ,
                                                "cliente_nif": object ,
                                                "poliza_negocio_codigo": object ,
                                                "poliza_negocio_padre_codigo": object ,
                                                "poliza_producto_tecnico": object,
                                                "poliza_producto_comercial": object,
                                                "cliente_codigo_postal": object,
                                                "cliente_codfiliacion": object ,
                                                "mediador_responsable_comercial_codigo": object ,
                                                "mediador_cp": object,
                                                "poliza_motivo_movimiento_codigo": object,
                                                "poliza_estructura_codigo": object,
                                                "poliza_movimiento_cod": object,
                                                "hogar_m2": object,
                                                "hogar_cp": object,
                                                "hogar_provincia":object,
                                                "hogar_anio_construccion": object,
                                                "garantia_robo_cobertura_metalico_cajafuerte": object,
                                                "garantia_robo_joyas_capital": object,
                                                "garantia_robo_objetos_capital": object,
                                                "garantia_robo_objetos_otros": object,
                                                "hogar_cod_poblacion": object}, index_col=None)

    file = file.replace('?', np.nan)

    return file


def load_file_HOGAR_matrix2(path_file, file_name):

    path = path_file + file_name
    file = pd.read_csv(path, sep=',', dtype={"poliza_id": object ,
                                                "cliente_nif": object ,
                                                "poliza_negocio_codigo": object ,
                                                "poliza_negocio_padre_codigo": object ,
                                                "poliza_producto_tecnico": object,
                                                "poliza_producto_comercial": object,
                                                "cliente_codigo_postal": object,
                                                "cliente_codfiliacion": object ,
                                                "mediador_responsable_comercial_codigo": object ,
                                                "mediador_cp": object,
                                                "poliza_motivo_movimiento_codigo": object,
                                                "poliza_estructura_codigo": object,
                                                "poliza_movimiento_cod": object
                                                #"hogar_m2": object,
                                                #"hogar_cp": object,
                                                #"hogar_provincia":object,
                                                #"hogar_anio_construccion": object,
                                                #"garantia_robo_cobertura_metalico_cajafuerte": object,
                                                #"garantia_robo_joyas_capital": object,
                                                #"garantia_robo_objetos_capital": object,"garantia_robo_objetos_otros": object
                                               }, index_col=None)

    file['poliza_fecha_cartera2'] = file['poliza_fecha_vencimiento_nat'] // 100
    fn = file_name[10:16]
    return file, fn


def load_file_AUTO2_matrix(path_file, file_name):

    path = path_file + file_name
    file = pd.read_csv(path, sep=',', dtype={"poliza_id": object ,
                                                "cliente_nif": object ,
                                                "poliza_negocio_codigo": object ,
                                                "poliza_negocio_padre_codigo": object ,
                                                "cliente_codigo_postal": object ,
                                                "cliente_codfiliacion": object ,
                                                "mediador_responsable_comercial_codigo": object ,
                                                "mediador_cp": object,
                                                "poliza_motivo_movimiento_codigo": object,
                                                "poliza_estructura_codigo": object,
                                                "poliza_movimiento_cod": object
                                                }, index_col=None)

    file = file.replace('?', np.nan)

    return file


def get_NIF(Botella):
    NIF = Botella[['cliente_codfiliacion','cliente_nif','cliente_codigo_tipodoc']]
    NIF = NIF.rename(columns={'cliente_codfiliacion': 'cl_codfiliacion' , 'cliente_nif': 'cl_nif', 'cliente_codigo_tipodoc':'cl_cod_tipodoc'})
    return NIF


def load_codunico_codf(path_file, file_name):
        header = ['cl_codfiliacion',
                  'cl_nif',
                  'cl_cod_tipodoc',
                  'correct_id',
                  'type_id',
                  'cod_global_unico',
                  'cod_fil_repetido']

        ruta = path_file + file_name

        file = pd.read_csv(ruta, names=header, dtype={"cod_global_unico": object, "cl_nif": object, "cl_codfiliacion": object},
                           index_col=None)

        #df = file.drop('column_name', 1)
        df1 = file[['cl_codfiliacion', 'cod_global_unico']]
        return df1


def read_codunico_home(path_file, file_name):

    path = path_file + file_name
    file = pd.read_csv(path, encoding="latin1", sep=';', dtype={"cl_codfiliacion": object,
                                                                "cl_nif": object,
                                                                "cl_cod_tipodoc": object}, index_col=None)

    file = file.rename(columns={'cl_codfiliacion': 'cliente_codfiliacion', 'cl_nif': 'cliente_nif', 'cl_cod_tipodoc': 'cliente_codigo_tipodoc'})
    file.loc[file['cod_fil_repetido'] >= 9, 'cod_global_unico'] = file['cliente_codfiliacion']
    #file = file[['cliente_codfiliacion', 'cod_global_unico']]
    return file


def load_codunico_nif(path_file, file_name):
        header = ['cl_codfiliacion',
                  'cl_nif',
                  'cl_cod_tipodoc',
                  'correct_id',
                  'type_id',
                  'cod_global_unico',
                  'cod_fil_repetido']

        ruta = path_file + file_name

        file = pd.read_csv(ruta, names=header,
                           dtype={"cod_global_unico": object, "cl_nif": object, "cl_codfiliacion": object},
                           index_col=None)

        # df = file.drop('column_name', 1)
        df1 = file[['cl_codfiliacion', 'cod_global_unico']]

def load_file_NoAUTO(path_file, file_name):
    header = ['p_id' ,
              'p_duracion' ,
              'p_estado' ,
              'p_version' ,
              'p_entidad_legal' ,
              'p_producto_tecnico' ,
              'p_producto_comercial' ,
              'p_fch_ultimo_reemplazo' ,
              'p_motivo_cancelacion_cod' ,
              'p_motivo_cancelacion_desc' ,
              'p_fch_efecto_nat' ,
              'p_fch_efecto_mov' ,
              'p_fch_emision_anulacion' ,
              'p_fch_cambio_estado' ,
              'p_fch_cartera' ,
              'p_fch_inicio' ,
              'p_fch_emision' ,
              'p_fch_vencimiento' ,
              'p_estructura_cod' ,
              'p_estructura_desc' ,
              'p_canal' ,
              'p_neg_cod' ,
              'p_neg_desc' ,
              'p_neg_padre_cod' ,
              'p_neg_padre_desc' ,
              'p_segmento_neg' ,
              'p_med' ,
              'audit_p_versiones' ,
              'audit_p_estados' ,
              'audit_p_tiene_ultima_situacion' ,
              'audit_p_tiene_anulacion' ,
              'audit_fch_cierre' ,
              'p_forma_pago_cod' ,
              'p_forma_pago_desc' ,
              'cl_nif' ,
              'cl_codfiliacion' ,
              'p_agrup_prod_cod' ,
              'cl_cod_tipodoc']

    ruta = path_file + file_name
    file = pd.read_csv(ruta , names=header , dtype={"p_id": object , "cl_nif": object , "cl_codfiliacion": object} ,
                       index_col=None)

    file.drop(['p_segmento_neg' , 'audit_p_versiones' , 'audit_p_estados' , 'audit_p_tiene_ultima_situacion' ,
               'audit_p_tiene_anulacion'] , inplace=True , axis=1)

    file['p_fch_cartera2'] = file['p_fch_vencimiento'] // 100  # Aqui tengo el AnoMEs de la cartera
    file['p_fch_cartera3'] = file['p_fch_cartera2'] % 100  # Aqui tengo el MES de la cartera
    file['p_fch_emision_anulacion2'] = file['p_fch_emision_anulacion'][0:6]  # Aqui tengo el MES de la cartera

    file['p_fch_inicio2'] = file['p_fch_inicio'] // 100  # Aqui AnoMes Inicio
    file['p_fch_cambio_estado2'] = file['p_fch_cambio_estado'] // 100  # Aqui AnoMes Cambio de Estado
    file['p_fch_efecto_nat2'] = file['p_fch_efecto_nat'] // 100  # Aqui AnoMes Fecha efecto natural

    file['p_producto_comercial2'] = file['p_producto_tecnico'].map(str) + ' ' + file['p_producto_comercial'].map(str)
    file['p_producto_comercial2'] = file['p_producto_comercial2'].astype(object)

    print("Aqui leyo la botella: NoAUTO")

    return file


def load_file_tenencia(path_file , file_name):
    header = ['p_id' ,
              'p_duracion' ,
              'p_estado' ,
              'p_version' ,
              'p_entidad_legal' ,
              'p_producto_tecnico' ,
              'p_producto_comercial' ,
              'p_fch_ultimo_reemplazo' ,
              'p_motivo_cancelacion_cod' ,
              'p_motivo_cancelacion_desc' ,
              'p_fch_efecto_nat' ,
              'p_fch_efecto_mov' ,
              'p_fch_emision_anulacion' ,
              'p_fch_cambio_estado' ,
              'p_fch_cartera' ,
              'p_fch_inicio' ,
              'p_fch_emision' ,
              'p_fch_vencimiento' ,
              'p_estructura_cod' ,
              'p_estructura_desc' ,
              'p_canal' ,
              'p_neg_cod' ,
              'p_neg_desc' ,
              'p_neg_padre_cod' ,
              'p_neg_padre_desc' ,
              'p_segmento_neg' ,
              'p_med' ,
              'audit_p_versiones' ,
              'audit_p_estados' ,
              'audit_p_tiene_ultima_situacion' ,
              'audit_p_tiene_anulacion' ,
              'audit_fch_cierre' ,
              'p_forma_pago_cod' ,
              'p_forma_pago_desc' ,
              'cl_nif' ,
              'cl_codfiliacion' ,
              'p_agrup_prod_cod' ,
              'cl_cod_tipodoc']

    ruta = path_file + file_name
    file = pd.read_csv(ruta , names=header , dtype={"p_id": object , "cl_nif": object , "cl_codfiliacion": object} ,
                       index_col=None)

    file.drop(['p_segmento_neg' , 'audit_p_versiones' , 'audit_p_estados' , 'audit_p_tiene_ultima_situacion' ,
               'audit_p_tiene_anulacion'] , inplace=True , axis=1)

    file['p_fch_cartera2'] = file['p_fch_vencimiento'] // 100  # Aqui tengo el AnoMEs de la cartera
    file['p_fch_cartera3'] = file['p_fch_cartera2'] % 100  # Aqui tengo el MES de la cartera
    file['p_fch_emision_anulacion2'] = file['p_fch_emision_anulacion'][0:6]  # Aqui tengo el MES de la cartera

    file['p_fch_inicio2'] = file['p_fch_inicio'] // 100  # Aqui AnoMes Inicio
    file['p_fch_cambio_estado2'] = file['p_fch_cambio_estado'] // 100  # Aqui AnoMes Cambio de Estado
    file['p_fch_efecto_nat2'] = file['p_fch_efecto_nat'] // 100  # Aqui AnoMes Fecha efecto natural

    df['p_producto_comercial2'] = df['p_producto_tecnico'].map(str) + ' ' + df['p_producto_comercial'].map(str)
    df['p_producto_comercial2'] = df['p_producto_comercial2'].astype(object)

    print("Aqui leyo la botella: tenencia")

    return file


def change_file(file):
    df = file
    #Tratar Tipo del vehículo
    df['vehi_tipo'] = None
    df.loc[df['vehi_clase_agrupacion_descripcion'] == 'Turismo' , 'vehi_tipo'] = '100'
    df.loc[df['vehi_clase_agrupacion_descripcion'] == 'Monovolumen' , 'vehi_tipo'] = '120'
    df.loc[df['vehi_clase_agrupacion_descripcion'] == 'Todo Terreno' , 'vehi_tipo'] = '150'
    df.loc[df['vehi_clase_agrupacion_descripcion'] == 'Comercial Derivado de Turismo' , 'vehi_tipo'] = '200'
    df.loc[df['vehi_clase_agrupacion_descripcion'] == 'Motocicletas' , 'vehi_tipo'] = '240'
    df.loc[df['vehi_clase_agrupacion_descripcion'] == 'Comercial Derivado de TT' , 'vehi_tipo'] = '250'
    df.loc[df['vehi_clase_agrupacion_descripcion'] == 'Ciclomotores' , 'vehi_tipo'] = '280'
    df.loc[df['vehi_clase_agrupacion_descripcion'] == 'Furgones y Camiones Ligeros' , 'vehi_tipo'] = '300'
    df.loc[df['vehi_clase_agrupacion_descripcion'] == 'Furgones Hab. Pasajeros' , 'vehi_tipo'] = '310'
    df.loc[df['vehi_clase_agrupacion_descripcion'] == 'Furgones pesados >3.5Tn' , 'vehi_tipo'] = '320'
    df.loc[df['vehi_clase_agrupacion_descripcion'] == 'Camiones>3,5 tn de PMA' , 'vehi_tipo'] = '350'
    df.loc[df['vehi_clase_agrupacion_descripcion'] == 'Tracto-Camiones' , 'vehi_tipo'] = '400'
    df.loc[df['vehi_clase_agrupacion_descripcion'] == 'Autocares y Autobuses' , 'vehi_tipo'] = '430'
    df.loc[df['vehi_clase_agrupacion_descripcion'] == 'Vehículos agrícolas' , 'vehi_tipo'] = '470'
    df.loc[df['vehi_clase_agrupacion_descripcion'] == 'Vehículos Industriales' , 'vehi_tipo'] = '500'
    df.loc[df['vehi_clase_agrupacion_descripcion'] == 'Remolques' , 'vehi_tipo'] = '900'
    df.loc[df['vehi_clase_agrupacion_descripcion'] == 'Semirremolques' , 'vehi_tipo'] = '910'

    #tratar comunidad autónoma




    df['p_canal'] = np.where(df['p_estructura_cod'] == 6300 , 'Mediacion' , 'Partners')

    df['p_producto_comercial2'] = df['p_producto_tecnico'].map(str) + ' ' + df['p_producto_comercial'].map(str)
    df['p_producto_comercial2'] = df['p_producto_comercial2'].astype(object)

    # df.loc[df['vehi_valor']=='?'] = ''
    df.vehi_valor[df.vehi_valor == '?'] = None
    pd.to_numeric(df['vehi_valor'] , errors='coerce')
    df['vehi_valor'] = df['vehi_valor'].convert_objects(convert_numeric=True)

    df.loc[df['vehi_potencia'] == '?'] = None
    pd.to_numeric(df['vehi_potencia'] , errors='coerce')
    # df['vehi_potencia'] = df['vehi_potencia'].convert_objects(convert_numeric=True)

    df.loc[df['vehi_fenacco_conductor1'] == '?' , 'vehi_fenacco_conductor1'] = None
    pd.to_numeric(df['vehi_fenacco_conductor1'] , errors='coerce')
    df['vehi_fenacco_conductor1'] = df['vehi_fenacco_conductor1'].convert_objects(convert_numeric=True)
    file['vehi_fenacco_conductor1_2'] = file['vehi_fenacco_conductor1'] // 100

    df.loc[df['vehi_fenacco_conductor2'] == '?' , 'vehi_fenacco_conductor2'] = None
    pd.to_numeric(df['vehi_fenacco_conductor2'] , errors='coerce')
    df['vehi_fenacco_conductor2'] = df['vehi_fenacco_conductor2'].convert_objects(convert_numeric=True)
    file['vehi_fenacco_conductor2_2'] = file['vehi_fenacco_conductor2'] // 100

    df.vehi_fepecon_conductor1 = pd.to_numeric(df.vehi_fepecon_conductor1 , errors='coerce').fillna(0).astype(np.int64)
    file['vehi_fepecon_conductor1_2'] = file['vehi_fepecon_conductor1'] // 100

    df.vehi_fepecon_conductor2 = pd.to_numeric(df.vehi_fepecon_conductor2 , errors='coerce').fillna(0).astype(np.int64)
    file['vehi_fepecon_conductor2_2'] = file['vehi_fepecon_conductor2'] // 100

    df.p_fch_emision_anulacion = pd.to_numeric(df.p_fch_emision_anulacion , errors='coerce').fillna(0).astype(np.int64)
    df['p_fch_emision_anulacion'] = df['p_fch_emision_anulacion'].convert_objects(convert_numeric=True)


    #Convertir el año de la matricula en entero
    df.loc[df['vehi_fch_mat'] == '?' , 'vehi_fch_mat'] = None
    pd.to_numeric(df['vehi_fch_mat'] , errors='coerce')
    df['vehi_fch_mat'] = df['vehi_fch_mat'].convert_objects(convert_numeric=True)

    df['p_estructura_cod'] = df['p_estructura_cod'].astype(str)

    df['med_clase'] = 'OTROS'
    df.loc[df['med_clase'] == 'Agente afecto' , 'med_tipo'] = 'AGENTES'
    df.loc[df['med_clase'] == 'Agente' , 'med_tipo'] = 'AGENTES'
    df.loc[df['med_clase'] == 'Agente exclusivo' , 'med_tipo'] = 'AGENTES'
    df.loc[df['med_clase'] == 'Corredor' , 'med_tipo'] = 'CORREDORES'
    df.loc[df['med_clase'] == 'Corredor o correduría en trámite con la DGS' , 'med_tipo'] = 'CORREDORES'
    df.loc[df['med_clase'] == 'Correduría con núm. DGS' , 'med_tipo'] = 'CORREDORES'

    return (df)



def load_tablon(path_file , file_name):
    ruta = path_file + file_name
    file = pd.read_csv(ruta , encoding="latin1", dtype={"p_id": object,
                                                        "cl_codfiliacion": object,
                                                        "p_canal": object,
                                                        "med_tipo": object,
                                                        "cl_sexo": object,
                                                        "cl_clase_persona_cod": object,
                                                        "cl_provincia": object,
                                                        "cl_ccaa": object,
                                                        "p_forma_pago_cod": object,
                                                        "p_modalidad_pack2": object,
                                                        "vehi_bonus_cod2": object,
                                                        "p_producto_tecnico": object,
                                                        "p_producto_comercial2": object,
                                                        "vehi_tipo": object,
                                                        "vehi_categoria": object,
                                                        "med_provincia": object,
                                                        "med_segmento_2": object,
                                                        "p_estado": object,
                                                        "p_motivo_cancelacion_cod": object,
                                                        "p_emision_canc": object,
                                                        "p_moment_canc": object,
                                                        "p_fch_cambio_estado2": object,
                                                        "p_motivo2": object,
                                                        "AUTO_siniestro_id_reciente": object,
                                                        "HOGAR_siniestro_id_reciente": object,
                                                        "cs_fch_p": object,
                                                        "cs_fch_todo": object,
                                                        "cs_id_siniesto_auto": object,
                                                        "cs_fch_siniestro": object,
                                                        "cs_id_siniestro_div": object,
                                                        "RAPPEL_NV_ABONADO2": object,
                                                        "RAPPEL_NV_OFERTADO2": object,
                                                        "cs_renewal": object,
                                                        }, index_col=None)

    print("Aqui leyo la botella: " + file_name)
    return file

