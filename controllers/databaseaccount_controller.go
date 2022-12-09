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
	"strings"
	"time"

	"github.com/kubeprj/database-operator/accountsvr"
	v1 "github.com/kubeprj/database-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	finalizerName      = "kubeprj.github.io/database-account-finalizer"
	defaultRequeueTime = 5 * time.Second
)

// DatabaseAccountReconciler reconciles a DatabaseAccount object.
type DatabaseAccountReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	Recorder      Recorder
	AccountServer *accountsvr.Server
	Config        *v1.DatabaseAccountControllerConfig
}

var ErrNewSecret = errors.New("creating new secret")
var ErrSecretImmutable = errors.New("secret is immutable")

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

		return ctrl.Result{}, err
	} else if apierrors.IsNotFound(err) {
		// record is deleted
		logger.V(1).Info("record is deleted")

		return ctrl.Result{}, nil
	}

	if ok, result, err := r.handleFinalizers(ctx, &dbAccount); ok {
		return result, err
	}

	// logger.V(1).Info("entering switch", "stage", dbAccount.Status.Stage)

	if r.Config.Debug.ReconcileSleep > 0 {
		//nolint:gosec,gomnd // simple random for use when debugging.
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

func (r *DatabaseAccountReconciler) handleFinalizers(
	ctx context.Context,
	dbAccount *v1.DatabaseAccount,
) (bool, ctrl.Result, error) {
	if dbAccount.ObjectMeta.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(dbAccount, finalizerName) {
			// logger.V(1).Info("adding finalizer to DatabaseAccount")
			controllerutil.AddFinalizer(dbAccount, finalizerName)
			if err := r.Update(ctx, dbAccount); err != nil {
				return true, ctrl.Result{}, err
			}
		}
	} else {
		// object is being deleted
		if controllerutil.ContainsFinalizer(dbAccount, finalizerName) {
			// our finalizer is present, so lets handle any external dependency
			if err := r.deleteExternalResources(ctx, dbAccount); err != nil {
				// if fail to delete the external dependency here, return with error
				// so that it can be retried
				return true, ctrl.Result{}, err
			}

			// remove our finalizer from the list and update it.
			controllerutil.RemoveFinalizer(dbAccount, finalizerName)
			if err := r.Update(ctx, dbAccount); err != nil {
				return true, ctrl.Result{}, err
			}
		}

		// Stop reconciliation as the item is being deleted
		return true, ctrl.Result{}, nil
	}

	return false, ctrl.Result{}, nil
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

		return r.deleteDatabase(ctx, name)
	case v1.OnDeleteRetain:
		logger.Info("Database record marked for retention, skipping delete")
	}
	return nil
}

// deleteExternalResources takes the DatabaseAccount and removes any external resources if required.
func (r *DatabaseAccountReconciler) deleteDatabase(ctx context.Context, name string) error {
	logger := log.FromContext(ctx)

	logger.Info("Database record marked for delete, deleting", "databaseName", name)

	dbName, ok, err := r.AccountServer.IsDatabase(ctx, name)
	switch {
	case err == nil && ok:
		logger.V(1).Info("Database exists, deleting")

		if err = r.AccountServer.Delete(ctx, name); err != nil {
			logger.Error(err, "Unable to delete database and/or user")

			return err
		}

		logger.Info("Database account deleted", "databaseName", name)
	case err != nil:
		logger.V(1).Error(err, "Unable to check if database exists")

		return err
	default:
		logger.V(1).Info("IsDatabase returned", "isDB[dbName]", dbName, "isDB[ok]", ok, "isDB[err]", err)
	}

	return nil
}

func (r *DatabaseAccountReconciler) stageZero(
	ctx context.Context,
	dbAccount *v1.DatabaseAccount,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	dbAccount.Status.Stage = v1.InitStage
	if len(dbAccount.Status.Name) == 0 {
		dbAccount.Status.Name = newDatabaseAccountName(ctx)
	}

	if err := dbAccount.UpdateStatus(ctx, r); err != nil {
		logger.V(1).Error(err, "unable to update DatabaseAccount")

		return ctrl.Result{}, err
	}

	// logger.V(1).Info("return result[ok]")
	return ctrl.Result{}, nil
}

func (r *DatabaseAccountReconciler) stageInit(
	ctx context.Context,
	dbAccount *v1.DatabaseAccount,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	err := secretRun(ctx, r, r, r.AccountServer, dbAccount, func(secret *corev1.Secret) error {
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
	})

	switch {
	case err != nil && errors.Is(err, ErrSecretImmutable):
		r.Recorder.WarningEvent(dbAccount, ReasonQueued, "Secret already exists and is immutable")
		dbAccount.Status.Error = true
		dbAccount.Status.ErrorMessage = "Secret already exists and is immutable"
		dbAccount.Status.Stage = v1.ErrorStage

		if err = dbAccount.UpdateStatus(ctx, r); err != nil {
			logger.V(1).Error(err, "Unable to update DatabaseAccount status")

			return ctrl.Result{}, err
		}

		return ctrl.Result{}, nil
	case err != nil:
		logger.V(1).Error(err, "Unable to create/retrieve secret")

		return ctrl.Result{}, err
	default:
		r.Recorder.NormalEvent(dbAccount, ReasonQueued, "Queued for creation")
	}

	dbAccount.Status.Stage = v1.UserCreateStage

	if err := r.Status().Update(ctx, dbAccount); err != nil {
		logger.V(1).Error(err, "Unable to update DatabaseAccount status")

		return ctrl.Result{}, err
	}

	// logger.V(1).Info("return result[ok]")
	return ctrl.Result{}, nil
}

func (r *DatabaseAccountReconciler) stageUserCreate(
	ctx context.Context,
	dbAccount *v1.DatabaseAccount,
) (ctrl.Result, error) {
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
		logger.V(1).Error(err, "Unable to create/retrieve secret")

		return ctrl.Result{}, err
	}

	dbAccount.Status.Stage = v1.DatabaseCreateStage

	if err := r.Status().Update(ctx, dbAccount); err != nil {
		logger.V(1).Error(err, "Unable to update DatabaseAccount")

		return ctrl.Result{}, err
	}
	r.Recorder.NormalEvent(dbAccount, ReasonDatabaseCreate, "Creating database")

	// logger.V(1).Info("return result[ok]")
	return ctrl.Result{}, nil
}

func (r *DatabaseAccountReconciler) stageDatabaseCreate(
	ctx context.Context,
	dbAccount *v1.DatabaseAccount,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	name, err := dbAccount.GetDatabaseName()
	if err != nil {
		return ctrl.Result{}, err
	}

	dbName, ok, err := r.AccountServer.IsDatabase(ctx, name)
	switch {
	case err != nil:
		r.Recorder.WarningEvent(
			dbAccount,
			ReasonDatabaseCreate,
			fmt.Sprintf("Failed to init check for create database: %s", err),
		)

		return ctrl.Result{}, err
	case ok:
		r.Recorder.WarningEvent(dbAccount, ReasonDatabaseCreate, "Database already exists")
		if err := secretRun(ctx, r, r, r.AccountServer, dbAccount, func(secret *corev1.Secret) error {
			r.setSecretKV(secret, accountsvr.DatabaseKeyDatabase, dbName)
			r.setSecretKV(secret, accountsvr.DatabaseKeyDSN, accountsvr.GenerateDSN(secret))

			boolTrue := true
			secret.Immutable = &boolTrue
			return nil
		}); err != nil {
			logger.V(1).Error(err, "Unable to update secret")

			return ctrl.Result{}, err
		}
	default:
		dbName, err := r.AccountServer.CreateDatabase(ctx, name, name)
		if err != nil {
			r.Recorder.WarningEvent(dbAccount, ReasonDatabaseCreate, fmt.Sprintf("Failed to create database: %s", err))
			return ctrl.Result{}, err
		}

		if err := secretRun(ctx, r, r, r.AccountServer, dbAccount, func(secret *corev1.Secret) error {
			r.setSecretKV(secret, accountsvr.DatabaseKeyDatabase, dbName)
			r.setSecretKV(secret, accountsvr.DatabaseKeyDSN, accountsvr.GenerateDSN(secret))

			boolTrue := true
			secret.Immutable = &boolTrue
			return nil
		}); err != nil {
			logger.V(1).Error(err, "Unable to update secret")

			return ctrl.Result{}, err
		}

		r.Recorder.NormalEvent(dbAccount, ReasonDatabaseCreate, "Database created")
	}

	dbAccount.Status.Stage = v1.ReadyStage
	dbAccount.Status.Ready = true
	if err := r.Status().Update(ctx, dbAccount); err != nil {
		logger.V(1).Error(err, "Unable to update DatabaseAccount")

		return ctrl.Result{}, err
	}

	r.Recorder.NormalEvent(dbAccount, ReasonReady, "Ready to use")
	logger.Info("Record is marked as ready",
		"databaseUsername", dbAccount.Status.Name,
		"secretName", dbAccount.GetSecretName(),
	)

	// logger.V(1).Info("return result[ok]")
	return ctrl.Result{}, nil
}

func (r *DatabaseAccountReconciler) stageReady(
	ctx context.Context,
	dbAccount *v1.DatabaseAccount,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	logger.V(1).Info("Record is marked as ready, nothing to do")

	if _, err := secretGetByName(ctx, r, dbAccount.GetSecretName()); apierrors.IsNotFound(err) {
		// secret has been deleted, probably sent here from reconcile trigger in secret delete.
		logger.Info("Secret has been deleted, remove DatabaseAccount")

		if err := r.Delete(ctx, dbAccount); err != nil {
			logger.Error(err, "Unable to delete DatabaseAccount in reaction to secret deletion")

			return ctrl.Result{RequeueAfter: defaultRequeueTime}, nil
		}

		return ctrl.Result{}, nil
	}

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
		logger.V(1).Error(err, "Unable to update secret")

		return ctrl.Result{}, err
	}

	if onDeleteUpdate {
		logger.Info("Database account onDelete changed, updated secret")
	}

	// logger.V(1).Info("return result[ok]")
	return ctrl.Result{}, nil
}

func (r *DatabaseAccountReconciler) stageError(
	ctx context.Context,
	dbAccount *v1.DatabaseAccount,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	logger.Info("Record is marked as error, nothing to do")

	return ctrl.Result{}, nil
}

func (r *DatabaseAccountReconciler) setSecretKV(secret *corev1.Secret, key, value string) {
	if secret.StringData == nil {
		secret.StringData = make(map[string]string)
	}
	if secret.Data == nil {
		secret.Data = make(map[string][]byte)
	}
	secret.Data[key] = []byte(value)
}

func (r *DatabaseAccountReconciler) reconcileSecret(secretObj client.Object) []reconcile.Request {
	ctx := context.Background()
	logger := log.FromContext(ctx)
	requests := []reconcile.Request{}

	name := types.NamespacedName{
		Name:      secretObj.GetName(),
		Namespace: secretObj.GetNamespace(),
	}
	secret, err := secretGetByName(ctx, r, name)
	if err != nil {
		return requests
	}

	if !strings.EqualFold(string(secret.Type), secretType) {
		return requests
	}
	// logger.Info("call():reconcileSecret",
	// 	"name", secretObj.GetName(),
	// 	"namespace", secretObj.GetNamespace(),
	// 	"managedFields", secretObj.GetManagedFields(),
	// 	"secretType", secret.Type,
	// )

	if secret.ObjectMeta.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(secret, finalizerName) {
			// logger.V(1).Info("adding finalizer to DatabaseAccount")
			controllerutil.AddFinalizer(secret, finalizerName)
			if err := r.Update(ctx, secret); err != nil {
				return requests
			}
		}
	} else {
		// object is being deleted
		if controllerutil.ContainsFinalizer(secret, finalizerName) {
			// our finalizer is present, so lets handle any external dependency
			if dbName, ok := secret.Data[accountsvr.DatabaseKeyDatabase]; ok {
				logger.Info("Database removal triggered by secret delete", "databaseName", dbName)

				if len(secret.GetOwnerReferences()) > 0 {
					ownerRef := secret.GetOwnerReferences()[0]
					requests = append(requests, reconcile.Request{
						NamespacedName: types.NamespacedName{
							Namespace: secret.GetNamespace(),
							Name:      ownerRef.Name,
						},
					})
				}

				if err := r.deleteDatabase(ctx, string(dbName)); err != nil {
					return requests
				}
			}

			// remove our finalizer from the list and update it.
			controllerutil.RemoveFinalizer(secret, finalizerName)
			if err := r.Update(ctx, secret); err != nil {
				return requests
			}
		}

		// Stop reconciliation as the item is being deleted
		return requests
	}

	return requests
}

// SetupWithManager sets up the controller with the Manager.
func (r *DatabaseAccountReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1.DatabaseAccount{}).
		Owns(&corev1.Secret{}).
		Watches(
			&source.Kind{Type: &corev1.Secret{}},
			handler.EnqueueRequestsFromMapFunc(r.reconcileSecret),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}),
		).
		Complete(r)
}
