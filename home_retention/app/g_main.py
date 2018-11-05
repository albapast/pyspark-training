import os
from home_retention.app import PATH_VAR
import pandas as pd
from home_retention.app import Home_process_pyspark



files = set([f for f in os.listdir(PATH_VAR.path_out)])
hogar_files = [i for i in files if 'snapshots' in i]

#   Extract those snapshots to read (12 to modeling + 3 to validate)

print(hogar_files)

data = pd.DataFrame()
for i in hogar_files:
    print("ENTRAR EN FOR...")
    print(i)
    hogar_file_new = i
    home = Home_process_pyspark.Home.home_bottle(hogar_file_new, test=True)
    print('After preprocesing', home.shape)
    home.to_csv(PATH_VAR.path_out2 + 'cartera' + '_target.csv', sep=';', index=False)

'''
    home = Home_process.Home.filtering(home, file_definitive)
    print('Done preprocessing home')

    cartera1 = Home_process.Home.get_cartera(home)  #tomo el subconjunto de la cartera

    print('Done getting cartera')

    #   Here we have to choose the target value, for this we should see the bottle of 4 months later.
    cartera_target = Home_process.Home.get_target_new(cartera1)

    print('Done getting target')

    cartera_target = Home_process.Home.get_tenencia(tenencia_clean, cartera_target)
    print('Done tenencia ', cartera_target.shape)
    cartera_target = Home_process.Home.get_anulaciones(tenencia_clean, cartera_target)
    print('Done anulaciones ', cartera_target.shape)
    cartera_target = Home_process.Home.get_claims(bottle_claims, cartera_target)
    print('Done Claims ', cartera_target.shape)
    cartera_target = Home_process.Home.get_premium(bottle_premium, cartera_target)
    print('Done Premium ', cartera_target.shape)

    #   Add the response time
    cartera_target = Home_process.Home.get_claim_response_time(cartera_target, bottle_claims, df_claim_guarantees)
    print('Done Po Reserva ', cartera_target.shape)

    print(cartera_target.head(5))

    print('The size of the cartera ', cartera_target.iloc[0]['audit_fecha_cierre_cartera'], ' es ',
          cartera_target.shape)
    cartera_loop = cartera_target.iloc[0]['audit_fecha_cierre_cartera']
    cartera_loop = str(cartera_loop)

    cartera_target.to_csv(PATH_VAR.path_out2 + 'cartera' + cartera_loop + '_target.csv', sep=';', index=False)
    data = data.append(cartera_target, ignore_index=True)

data.to_csv(PATH_VAR.path_out2 + 'Total_carteras.csv', encoding="latin1",  sep=';', index=False)
print('Termino tablon cartera  ', data.shape)

tablon_inicial = PostProcess.PostProcessTablon.tablon_inicial(data)
tablon_inicial.to_csv(PATH_VAR.path_out2 + 'Tablon_hogar_inicial.csv', encoding="latin1", sep=';', index=False)
tablon_modificado = PostProcess.PostProcessTablon.modificacion_tablon(tablon_inicial)

data_clean = PostProcess.PostProcessTablon.clean_migration(tablon_modificado)

#   Split the sample by channel
agentes, corredores, partners = PostProcess.PostProcessTablon.get_tablon_by_chanel(data_clean)

#   Here the Model
#model_agente.model_agentes(agentes)

Models_basic.Models.model_ensemble(agentes)'''

