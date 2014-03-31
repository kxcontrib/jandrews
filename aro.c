/*         aro.c  a kdb+ extension to query Oracle databases 
                  - jack@ivorykite.com
 aro.so only depends on two files from oracle: libclntsh.so libnnz11.so
 aro fetches a query as a table of strings which is nice for a quick POC without setting up ODBC
 for a more odbc.q-like interface, use ODBC

q)q:(`aro 2:(`qry;3))[("scott";"tiger";"localhost/XE")]
q)q["select table_name from user_tables";20]
"T1" "T2" "S1" "M1" ,"T"
q)"P"$0N!q["select TO_CHAR(SYSTIMESTAMP,'YYYY.MM.DD\"D\"HH24:MI:SS.FF') t from dual";1]
,,"2014.03.31D13:53:30.771712"
2014.03.31D13:53:30.771712000

 to build:
 . either an oracle client exists, or download an instant client from oracle and extract to $IC
 . download the sdk and extract to $IC/sdk
 . in the shell:
for x in clntsh nnz11; do ln -s $IC/lib$x.so* lib$x.so; done
gcc -m32 -shared -fPIC aro.c -I $IC/sdk/include -I $QHOME -g -o aro.so -L. -lnnz11 -lclntsh
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <oci.h>
#define KXVER 3
#include <k.h>
typedef unsigned UI;typedef unsigned long long UJ;typedef unsigned char UC;
#ifdef _LP64
#define AR 8
typedef UJ W;
#else
#define AR 4
typedef UI W;
#endif
#define RZ R 0;
#define ZO sizeof
#define ZW ZO(W)
V*ma(I n){V*x;memset(x=malloc(n),0,n);R x;}fr(V*x){free(x);RZ}
A_(S e,S f,I l){__assert_fail(e,f,l,"function");R 0;}
S Os(S f,...){S s=0;va_list v;va_start(v,f);vasprintf(&s,f,v);va_end(v);R s;}
#define AI(e,f,b){I g=(e),h=(f);if((g==h)!=b){S s=Os("%s(%d)%s%s(%d)",#e,g,b?"==":"!=",#f,h);A_(s,__FILE__, __LINE__);fr(s);}}
#define EQ(e,f) AI((e),(f),1)
#define NE(e,f) AI((e),(f),0)
#define A(e)    NE(!!(e),0)
FILE*fop(S fn){R fopen(fn,"w");}
Z OCIEnv*nv;Z OCIError*er;Z OCIServer*srvhp;Z OCISvcCtx*svchp;Z OCISession*authp;
#define OEZ 512
ZJ ker(S oeb,OCIError *er,sword status)
{sb4 e=oeb[0]=0;if(!status)RZ;I n=0;
 switch(status)
#define OCI_X(x) CS(OCI_##x,n=snprintf(oeb,OEZ,"Error - " #x "\n"))
 {OCI_X(ERROR);OCI_X(SUCCESS);OCI_X(SUCCESS_WITH_INFO);OCI_X(NEED_DATA);OCI_X(NO_DATA);
  OCI_X(INVALID_HANDLE);OCI_X(STILL_EXECUTING);OCI_X(CONTINUE);
 }OCIErrorGet(er,(I)1,(text*)0,&e,oeb+n,(I)sizeof(oeb)-n,OCI_HTYPE_ERROR);
 R e;
}
#define Oe(...) (fflush(stderr),fprintf(stderr,__VA_ARGS__),fflush(stderr))
ZC oeb[OEZ];
Z eo(I x,I y,I z,S s,S f,I l){if(x){*oeb=0;if(y){ker(oeb,er,x);Oe("%s",oeb);};Oe("  %s:%d\n",f,l);*oeb?(I)krr(oeb):Oe("internal\n");}R x;}
#define EO(X,Y,Z) {if(eo(X,Y,Z,#X,__FILE__,__LINE__))R kb(0);}
#define B(X)    EO((X),1,1)
#define oha(par,hnd,type,xtramem_sz,usrmempp) OCIHandleAlloc((dvoid*)par,(dvoid**)hnd,type,xtramem_sz,(dvoid**)usrmempp)
#define ohf(x,y) OCIHandleFree((dvoid*)x,y)
#define oas OCIAttrSet
ZK ope(K x,K y,K z)//"user" "pass" "host[:port][/service name]"
{
 B(OCIEnvCreate(&nv,0,0,0,0,0,0,0));
 B(oha(nv,&er,OCI_HTYPE_ERROR,0,0));
 B(oha(nv,&srvhp,OCI_HTYPE_SERVER,0,0));
 B(oha(nv,&svchp,OCI_HTYPE_SVCCTX,0,0));
 B(OCIServerAttach(srvhp,er,kC(z),z->n,0));
 B(oas(svchp,OCI_HTYPE_SVCCTX,srvhp,0,OCI_ATTR_SERVER,er));
 B(oha(nv,&authp,OCI_HTYPE_SESSION,0,0));
 B(oas(authp,OCI_HTYPE_SESSION,xC,xn,OCI_ATTR_USERNAME,er));
 B(oas(authp,OCI_HTYPE_SESSION,kC(y),y->n,OCI_ATTR_PASSWORD,er));
 B(OCISessionBegin(svchp,er,authp,OCI_CRED_RDBMS,OCI_DEFAULT));
 B(oas(svchp,OCI_HTYPE_SVCCTX,authp,0,OCI_ATTR_SESSION,er));
 RZ
}
K qry(K x,K y,K z)//(user;pass;host) query maxrows
{A(xt==0&&xn==3);
 A(!ope(xx,xy,xK[2]));
 K r=0;J m=z->j,n=0;
 OCIStmt*s;B(OCIStmtPrepare2(svchp,&s,er,kC(y),y->n,0,0,OCI_NTV_SYNTAX,0));
 OCIStmtExecute(svchp,s,er,0,0,0,0,0);
 OCIParam*mypard=0;for(;!OCIParamGet(s,OCI_HTYPE_STMT,er,(void**)&mypard,n+1);n++);
 I j,f=0,g;ub2**l;K*ps;
 l=(ub2**)ma(ZW*n);DO(n,l[i]=ma(ZO(**l)*m))
#define K0(...) k(0,__VA_ARGS__,((K)0))
 r=K0("{(x;y*32)#10h$0}",kj(n),kj(m));
 DO(n,B(OCIDefineByPos(s,ma(ZW),er,i+1,kC(kK(r)[i]),32,SQLT_STR,0,l[i],0,0)))
 OCIStmtFetch2(s,er,m,OCI_DEFAULT,0,0);
 B(OCIAttrGet(s,OCI_HTYPE_STMT,(void*)&f,NULL,OCI_ATTR_ROWS_FETCHED,er));
#define FO(i,n) for(i=0;i<(n);i++) 
 K w=ktn(0,n);DO(n,kK(w)[i]=ktn(KI,f));FO(j,n)DO(f,kI(kK(w)[j])[i]=l[j][i]);DO(n,fr(l[i]));fr(l);
 r=K0("k){x#''#[(y;32)]'z}",w,kj(f),r);
 ohf(s,OCI_HTYPE_STMT);
 B(OCISessionEnd(svchp,er,authp,(ub4)0));
 B(OCIServerDetach(srvhp,er,0));
 B(ohf(nv,OCI_HTYPE_ENV));
 R r;
}
