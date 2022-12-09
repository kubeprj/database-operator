/*
Copyright 2022 na4ma4.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/kubeprj/database-operator/accountsvr"
	kubeprjgithubiov1 "github.com/kubeprj/database-operator/api/v1"
	v1 "github.com/kubeprj/database-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	finalizerName = "kubeprj.github.io/database-account-finalizer"
)

// DatabaseAccountReconciler reconciles a DatabaseAccount object
type DatabaseAccountReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	Recorder      Recorder
	AccountServer *accountsvr.Server
	Config        *v1.DatabaseAccountControllerConfig
}

var ErrNewSecret = errors.New("creating new secret")
var ErrSecretImmutable = errors.New("secret is immutable")
var requeueResult = ctrl.Result{RequeueAfter: 5 * time.Second}

//+kubebuilder:rbac:groups=kubeprj.github.io,resources=databaseaccounts,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=kubeprj.github.io,resources=databaseaccounts/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=kubeprj.github.io,resources=databaseaccounts/finalizers,verbs=update
//+kubebuilder:rbac:groups=kubeprj.github.io,resources=events,verbs=create;patch
//+kubebuilder:rbac:groups="",resources=events,verbs=create;patch
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the DatabaseAccount object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.13.0/pkg/reconcile
func (r *DatabaseAccountReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// logger := log.FromContext(ctx).WithValues("name", req.NamespacedName.String())
	// ctx = log.IntoContext(ctx, logger)
	logger := log.FromContext(ctx)

	// logger.V(1).Info("call():Reconcile")

	var dbAccount v1.DatabaseAccount
	if err := r.Get(ctx, req.NamespacedName, &dbAccount); err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "unable to retrieve database account")

		return requeueResult, err
	} else if apierrors.IsNotFound(err) {
		// record is deleted
		logger.V(1).Info("record is deleted")

		return ctrl.Result{}, nil
	}

	if dbAccount.ObjectMeta.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(&dbAccount, finalizerName) {
			// logger.V(1).Info("adding finalizer to DatabaseAccount")
			controllerutil.AddFinalizer(&dbAccount, finalizerName)
			if err := r.Update(ctx, &dbAccount); err != nil {
				return ctrl.Result{}, err
			}
		}
	} else {
		// object is being deleted
		if controllerutil.ContainsFinalizer(&dbAccount, finalizerName) {
			// our finalizer is present, so lets handle any external dependency
			if err := r.deleteExternalResources(ctx, &dbAccount); err != nil {
				// if fail to delete the external dependency here, return with error
				// so that it can be retried
				return requeueResult, err
			}

			// remove our finalizer from the list and update it.
			controllerutil.RemoveFinalizer(&dbAccount, finalizerName)
			if err := r.Update(ctx, &dbAccount); err != nil {
				return ctrl.Result{}, err
			}
		}

		// Stop reconciliation as the item is being deleted
		return ctrl.Result{}, nil
	}

	// logger.V(1).Info("entering switch", "stage", dbAccount.Status.Stage)

	if r.Config.Debug.ReconcileSleep > 0 {
		n := rand.Intn(r.Config.Debug.ReconcileSleep/2) + r.Config.Debug.ReconcileSleep/2
		logger.V(1).Info(fmt.Sprintf("sleeping %d seconds", n))
		time.Sleep(time.Duration(n) * time.Second)
	}

	switch dbAccount.Status.Stage {
	case v1.UnknownStage:
		return r.stageZero(ctx, &dbAccount)
	case v1.InitStage:
		return r.stageInit(ctx, &dbAccount)
	case v1.UserCreateStage:
		return r.stageUserCreate(ctx, &dbAccount)
	case v1.DatabaseCreateStage:
		return r.stageDatabaseCreate(ctx, &dbAccount)
	case v1.ReadyStage:
		return r.stageReady(ctx, &dbAccount)
	case v1.ErrorStage:
		return r.stageError(ctx, &dbAccount)
	default:
		logger.Error(nil, "Unknown error type", "stage_value", dbAccount.Status.Stage)
	}

	// logger.V(1).Info("Fallback return")
	return ctrl.Result{}, nil
}

// deleteExternalResources takes the DatabaseAccount and removes any external resources if required.
func (r *DatabaseAccountReconciler) deleteExternalResources(ctx context.Context, dbAccount *v1.DatabaseAccount) error {
	logger := log.FromContext(ctx)

	switch dbAccount.Spec.OnDelete {
	case v1.OnDeleteDelete:
		name, err := dbAccount.GetDatabaseName()
		if err != nil {
			return err
		}

		logger.Info("Database record marked for delete, deleting", "dbName", name)

		if dbName, ok, err := r.AccountServer.IsDatabase(ctx, name); err == nil && ok {
			logger.V(1).Info("Database exists, deleting")

			if err := r.AccountServer.Delete(ctx, name); err != nil {
				logger.Error(err, "Unable to delete database and/or user")

				return err
			}
		} else if err != nil {
			logger.Error(err, "Unable to check if database exists")

			return err
		} else {
			logger.V(1).Info("IsDatabase returned", "isDB[dbName]", dbName, "isDB[ok]", ok, "isDB[err]", err)
		}
	case v1.OnDeleteRetain:
		logger.Info("database record marked for retention, skipping delete")
	}
	return nil
}

func (r *DatabaseAccountReconciler) stageZero(ctx context.Context, dbAccount *v1.DatabaseAccount) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	dbAccount.Status.Stage = v1.InitStage
	if len(dbAccount.Status.Name) == 0 {
		dbAccount.Status.Name = newDatabaseAccountName(ctx)
	}

	if err := dbAccount.UpdateStatus(r, ctx); err != nil {
		logger.Error(err, "unable to update DatabaseAccount")

		return requeueResult, err
	}

	// logger.V(1).Info("return result[ok]")
	return ctrl.Result{}, nil
}

func (r *DatabaseAccountReconciler) stageInit(ctx context.Context, dbAccount *v1.DatabaseAccount) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if err := secretRun(ctx, r, r, r.AccountServer, dbAccount, func(secret *corev1.Secret) error {
		return nil
	}); err != nil && errors.Is(err, ErrSecretImmutable) {

	}

	if err := secretRun(ctx, r, r, r.AccountServer, dbAccount, func(secret *corev1.Secret) error {
		if secret.Immutable != nil && *secret.Immutable {
			return ErrSecretImmutable
		}

		name, err := dbAccount.GetDatabaseName()
		if err != nil {
			return err
		}

		r.AccountServer.CopyConfigToSecret(secret)
		r.setSecretKV(secret, accountsvr.DatabaseKeyUsername, name)

		return nil
	}); err != nil && errors.Is(err, ErrSecretImmutable) {
		r.Recorder.WarningEvent(dbAccount, ReasonQueued, "Secret already exists and is immutable")
		dbAccount.Status.Error = true
		dbAccount.Status.ErrorMessage = "Secret already exists and is immutable"
		dbAccount.Status.Stage = v1.ErrorStage

		if err := dbAccount.UpdateStatus(r, ctx); err != nil {
			logger.Error(err, "Unable to update DatabaseAccount status")

			return requeueResult, err
		}

		return ctrl.Result{}, nil
	} else if err != nil {
		logger.Error(err, "Unable to create/retrieve secret")

		return requeueResult, err
	} else {
		r.Recorder.NormalEvent(dbAccount, ReasonQueued, "Queued for creation")
	}

	dbAccount.Status.Stage = v1.UserCreateStage

	if err := r.Status().Update(ctx, dbAccount); err != nil {
		logger.Error(err, "Unable to update DatabaseAccount status")

		return requeueResult, err
	}

	// logger.V(1).Info("return result[ok]")
	return ctrl.Result{}, nil
}

func (r *DatabaseAccountReconciler) stageUserCreate(ctx context.Context, dbAccount *v1.DatabaseAccount) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	r.Recorder.NormalEvent(dbAccount, ReasonUserCreate, "Creating database user")

	if err := secretRun(ctx, r, r, r.AccountServer, dbAccount, func(secret *corev1.Secret) error {
		name, err := dbAccount.GetDatabaseName()
		if err != nil {
			return err
		}

		usr, pw, err := r.AccountServer.CreateRole(ctx, name)
		if errors.Is(err, accountsvr.ErrRoleExists) {
			r.Recorder.WarningEvent(dbAccount, ReasonUserCreate, "User exists, creating new password")
			usr, pw, err = r.AccountServer.UpdateRolePassword(ctx, name)
		}
		if err != nil {
			r.Recorder.WarningEvent(dbAccount, ReasonUserCreate, fmt.Sprintf("Failed to create user: %s", err))

			return err
		}

		r.Recorder.NormalEvent(dbAccount, ReasonUserCreate, "User created")

		r.setSecretKV(secret, accountsvr.DatabaseKeyUsername, usr)
		r.setSecretKV(secret, accountsvr.DatabaseKeyPassword, pw)

		return nil
	}); err != nil {
		logger.Error(err, "Unable to create/retrieve secret")

		return requeueResult, err
	}

	dbAccount.Status.Stage = v1.DatabaseCreateStage

	if err := r.Status().Update(ctx, dbAccount); err != nil {
		logger.Error(err, "Unable to update DatabaseAccount")

		return requeueResult, err
	}
	r.Recorder.NormalEvent(dbAccount, ReasonDatabaseCreate, "Creating database")

	// logger.V(1).Info("return result[ok]")
	return ctrl.Result{}, nil
}

func (r *DatabaseAccountReconciler) stageDatabaseCreate(ctx context.Context, dbAccount *v1.DatabaseAccount) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	name, err := dbAccount.GetDatabaseName()
	if err != nil {
		return ctrl.Result{}, err
	}

	if dbName, ok, err := r.AccountServer.IsDatabase(ctx, name); err != nil {
		r.Recorder.WarningEvent(dbAccount, ReasonDatabaseCreate, fmt.Sprintf("Failed to init check for create database: %s", err))

		return ctrl.Result{}, err
	} else if ok {
		r.Recorder.WarningEvent(dbAccount, ReasonDatabaseCreate, "Database already exists")
		if err := secretRun(ctx, r, r, r.AccountServer, dbAccount, func(secret *corev1.Secret) error {
			r.setSecretKV(secret, accountsvr.DatabaseKeyDatabase, dbName)
			r.setSecretKV(secret, accountsvr.DatabaseKeyDSN, accountsvr.GenerateDSN(secret))

			boolTrue := true
			secret.Immutable = &boolTrue
			return nil
		}); err != nil {
			logger.Error(err, "Unable to update secret")

			return requeueResult, err
		}
	} else {
		dbName, err := r.AccountServer.CreateDatabase(ctx, name, name)

		if err := secretRun(ctx, r, r, r.AccountServer, dbAccount, func(secret *corev1.Secret) error {
			r.setSecretKV(secret, accountsvr.DatabaseKeyDatabase, dbName)
			r.setSecretKV(secret, accountsvr.DatabaseKeyDSN, accountsvr.GenerateDSN(secret))

			boolTrue := true
			secret.Immutable = &boolTrue
			return nil
		}); err != nil {
			logger.Error(err, "Unable to update secret")

			return requeueResult, err
		}

		if err != nil {
			r.Recorder.WarningEvent(dbAccount, ReasonDatabaseCreate, fmt.Sprintf("Failed to create database: %s", err))
			return ctrl.Result{}, err
		}

		r.Recorder.NormalEvent(dbAccount, ReasonDatabaseCreate, "Database created")
	}

	dbAccount.Status.Stage = v1.ReadyStage
	dbAccount.Status.Ready = true
	if err := r.Status().Update(ctx, dbAccount); err != nil {
		logger.Error(err, "Unable to update DatabaseAccount")

		return requeueResult, err
	}

	r.Recorder.NormalEvent(dbAccount, ReasonReady, "Ready to use")

	// logger.V(1).Info("return result[ok]")
	return ctrl.Result{}, nil
}

func (r *DatabaseAccountReconciler) stageReady(ctx context.Context, dbAccount *v1.DatabaseAccount) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	logger.Info("Record is marked as ready, nothing to do")

	onDeleteUpdate := false
	if err := secretRun(ctx, r, r, r.AccountServer, dbAccount, func(secret *corev1.Secret) error {
		// logger.V(1).Info("Checking database account spec")
		// logger.V(1).Info("Database account spec", "dbAccount.Spec.OnDelete", dbAccount.Spec.OnDelete)
		switch dbAccount.Spec.OnDelete {
		case v1.OnDeleteDelete:
			if len(secret.ObjectMeta.OwnerReferences) != 1 {
				// logger.V(1).Info("Database account marked for deletion but secret does not have ownerreferences")
				secretAddOwnerRefs(secret, dbAccount)
				onDeleteUpdate = true
			}
		case v1.OnDeleteRetain:
			if len(secret.ObjectMeta.OwnerReferences) != 0 {
				// logger.V(1).Info("Database account marked for retention but secret has ownerreferences")
				secret.OwnerReferences = nil
				onDeleteUpdate = true
			}
		}

		return nil
	}); err != nil {
		logger.Error(err, "Unable to update secret")

		return requeueResult, err
	}

	if onDeleteUpdate {
		logger.Info("Database account onDelete changed, updated secret")
	}

	// logger.V(1).Info("return result[ok]")
	return ctrl.Result{}, nil
}

func (r *DatabaseAccountReconciler) stageError(ctx context.Context, dbAccount *v1.DatabaseAccount) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	logger.Info("Record is marked as error, nothing to do")

	return ctrl.Result{}, nil
}

// func (r *DatabaseAccountReconciler) addOwnerRefs(secret *corev1.Secret, dbAccount *v1.DatabaseAccount) *corev1.Secret {
// 	secret.OwnerReferences = []metav1.OwnerReference{*dbAccount.GetReference()}

// 	return secret
// }

// func (r *DatabaseAccountReconciler) getSecret(ctx context.Context, dbAccount *v1.DatabaseAccount) (*corev1.Secret, error) {
// 	secret := &corev1.Secret{}

// 	if err := r.Get(ctx, dbAccount.GetSecretName(), secret); apierrors.IsNotFound(err) {

// 		// user := uuid.NewUUID()
// 		secret = &corev1.Secret{
// 			ObjectMeta: metav1.ObjectMeta{
// 				Name:        dbAccount.GetSecretName().Name,
// 				Namespace:   dbAccount.Namespace,
// 				Annotations: dbAccount.Spec.SecretTemplate.Annotations,
// 				Labels:      dbAccount.Spec.SecretTemplate.Labels,
// 			},
// 			StringData: map[string]string{},
// 			Data:       map[string][]byte{},
// 			Type:       `kubeprj.github.io/database-account`,
// 		}
// 		if dbAccount.Spec.OnDelete == v1.OnDeleteDelete {
// 			r.addOwnerRefs(secret, dbAccount)
// 		}

// 		return secret, ErrNewSecret
// 	} else if err != nil {
// 		return secret, err
// 	}

// 	return secret, nil
// }

func (r *DatabaseAccountReconciler) setSecretKV(secret *corev1.Secret, key, value string) {
	if secret.StringData == nil {
		secret.StringData = make(map[string]string)
	}
	if secret.Data == nil {
		secret.Data = make(map[string][]byte)
	}
	secret.Data[key] = []byte(value)
}

// func (r *DatabaseAccountReconciler) databaseNameFromAccount(dbAccount *v1.DatabaseAccount) (string, error) {
// 	if len(dbAccount.Status.Name) == 0 {
// 		return "", fmt.Errorf("%w: database username missing", ErrInvalidSecret)
// 	}

// 	return dbAccount.Status.Name.String(), nil
// }

// func (r *DatabaseAccountReconciler) databaseNameFromSecret(secret *corev1.Secret) (string, error) {
// 	if v, ok := secret.Data[accountsvr.DatabaseKeyUsername]; ok {
// 		return string(v), nil
// 	}

// 	return "", fmt.Errorf("%w: database username missing", ErrInvalidSecret)
// }

// func (r *DatabaseAccountReconciler) secretNameFromAccount(dbAccount *v1.DatabaseAccount) types.NamespacedName {
// 	if dbAccount.Spec.SecretName != "" {
// 		return types.NamespacedName{
// 			Namespace: dbAccount.GetNamespace(),
// 			Name:      dbAccount.Spec.SecretName,
// 		}
// 	}

// 	return types.NamespacedName{
// 		Namespace: dbAccount.GetNamespace(),
// 		Name:      dbAccount.GetName(),
// 	}
// }

// func (r *DatabaseAccountReconciler) applySecretChanges(ctx context.Context, dbAccount *v1.DatabaseAccount, f func(secret *corev1.Secret) error) error {
// 	logger := log.FromContext(ctx)

// 	secret, err := secretGet(ctx, r, dbAccount)
// 	if errors.Is(err, ErrNewSecret) {
// 		r.AccountServer.CopyConfigToSecret(secret)
// 		if err := r.Create(ctx, secret); err != nil {
// 			logger.Error(err, "unable to create secret")

// 			return err
// 		}
// 	} else if err != nil {
// 		return err
// 	}

// 	preChecksum := crc32.ChecksumIEEE([]byte(secret.String()))

// 	if err := f(secret); err != nil {
// 		return err
// 	}

// 	postChecksum := crc32.ChecksumIEEE([]byte(secret.String()))

// 	// logger.V(1).Info("Checksum Comparison", "preChecksum", preChecksum, "postChecksum", postChecksum)

// 	if preChecksum != postChecksum {
// 		logger.V(1).Info("secret updated", "preChecksum", preChecksum, "postChecksum", postChecksum)
// 		if err := r.Update(ctx, secret); err != nil {
// 			logger.Error(err, "unable to update secret")

// 			return err
// 		}
// 	}

// 	return nil
// }

// SetupWithManager sets up the controller with the Manager.
func (r *DatabaseAccountReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kubeprjgithubiov1.DatabaseAccount{}).
		Complete(r)
}
