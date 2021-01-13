# web-crawler utilizando chilkat-lib

## Configurações de ambiente
É necessário configurar as variáveis abaixo, pois o Makefile fará uso delas.
- CHILKAT_PATH = Caminho até o a Chilkat Lib.
- LD_LIBRARY_PATH = Caminho até os binários da chilkat. Normalmente na pasta lib.

## Execução
Com as variaveis de ambiente devidamente configuradas, execute ```make``` dentro da raiz do projeto.
Em seguida:
```bash 
./crawler < seeds.txt
```
Sendo ```seeds.txt``` a lista com as URLs seed, uma por linha.