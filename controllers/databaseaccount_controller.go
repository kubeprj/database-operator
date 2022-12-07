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
	"time"

	"github.com/kubeprj/database-operator/accountsvr"
	kubeprjgithubiov1 "github.com/kubeprj/database-operator/api/v1"
	v1 "github.com/kubeprj/database-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// DatabaseAccountReconciler reconciles a DatabaseAccount object
type DatabaseAccountReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	Recorder      record.EventRecorder
	AccountServer *accountsvr.Server
}

var ErrNewSecret = errors.New("creating new secret")

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
	logger := log.FromContext(ctx)

	var dbAccount v1.DatabaseAccount
	if err := r.Get(ctx, req.NamespacedName, &dbAccount); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("removing database and user", "name", req.Name)
			if err := r.AccountServer.Delete(ctx, req.Name); err != nil {
				logger.Error(err, "unable to delete database and/or user")

				return ctrl.Result{RequeueAfter: 5 * time.Second}, err
			}

			return ctrl.Result{}, nil
		}

		logger.Error(err, "unable to fetch DatabaseAccount")

		return ctrl.Result{}, err
	}

	switch dbAccount.Status.Stage {
	case v1.UnknownStage:
		dbAccount.Status.Stage = v1.InitStage

		if err := r.Status().Update(ctx, &dbAccount); err != nil {
			logger.Error(err, "unable to update DatabaseAccount")

			return ctrl.Result{
				RequeueAfter: 5 * time.Second,
			}, err
		}

		r.Recorder.Event(&dbAccount, "Normal", "Queued", "Queued for creation")

		return ctrl.Result{}, nil
	case v1.InitStage:
		return r.stageInit(ctx, &dbAccount)
	case v1.UserCreateStage:
		return r.stageUserCreate(ctx, &dbAccount)
	case v1.DatabaseCreateStage:
		return r.stageDatabaseCreate(ctx, &dbAccount)
	case v1.ReadyStage:
		return r.stageReady(ctx, &dbAccount)
	default:
		logger.Error(nil, "Unknown error type", "stage_value", dbAccount.Status.Stage)
	}

	return ctrl.Result{}, nil
}

func (r *DatabaseAccountReconciler) stageInit(ctx context.Context, dbAccount *v1.DatabaseAccount) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	secret, err := r.getSecret(ctx, dbAccount)
	if errors.Is(err, ErrNewSecret) {
		r.AccountServer.CopyConfigToSecret(secret)
		if err := r.Create(ctx, secret); err != nil {
			logger.Error(err, "unable to update Secret")

			return ctrl.Result{
				RequeueAfter: 5 * time.Second,
			}, err
		}
	} else if err == nil {
		r.AccountServer.CopyConfigToSecret(secret)
		if err := r.Update(ctx, secret); err != nil {
			logger.Error(err, "unable to update Secret")

			return ctrl.Result{
				RequeueAfter: 5 * time.Second,
			}, err
		}
	} else if err != nil {
		logger.Error(err, "unable to create/retrieve secret")
	}

	dbAccount.Status.Stage = v1.UserCreateStage

	if err := r.Status().Update(ctx, dbAccount); err != nil {
		logger.Error(err, "unable to update DatabaseAccount")

		return ctrl.Result{
			RequeueAfter: 5 * time.Second,
		}, err
	}
	r.Recorder.Event(dbAccount, "Normal", "UserCreate", "Creating Database user")

	return ctrl.Result{}, nil
}

func (r *DatabaseAccountReconciler) stageUserCreate(ctx context.Context, dbAccount *v1.DatabaseAccount) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	secret, err := r.getSecret(ctx, dbAccount)
	if err != nil {
		logger.Error(err, "unable to create/retrieve secret")
	}

	usr, pw, err := r.AccountServer.CreateRole(ctx, dbAccount.Name)
	if errors.Is(err, accountsvr.ErrRoleExists) {
		r.Recorder.Event(dbAccount, "Warning", "UserCreate", "User exists, creating new password")
		usr, pw, err = r.AccountServer.UpdateRolePassword(ctx, dbAccount.Name)
	}
	if err != nil {
		r.Recorder.Event(dbAccount, "Warning", "UserCreate", fmt.Sprintf("Failed to create user: %s", err))

		return ctrl.Result{}, err
	}

	r.Recorder.Event(dbAccount, "Normal", "UserCreate", "User created")

	r.setSecretKV(secret, accountsvr.DatabaseKeyUsername, usr)
	r.setSecretKV(secret, accountsvr.DatabaseKeyPassword, pw)
	dbAccount.Status.Stage = v1.DatabaseCreateStage

	if err := r.Status().Update(ctx, dbAccount); err != nil {
		logger.Error(err, "unable to update DatabaseAccount")

		return ctrl.Result{
			RequeueAfter: 5 * time.Second,
		}, err
	}
	r.Recorder.Event(dbAccount, "Normal", "DatabaseCreate", "Creating Database")

	if err := r.Update(ctx, secret); err != nil {
		logger.Error(err, "unable to update Secret")

		return ctrl.Result{
			RequeueAfter: 5 * time.Second,
		}, err
	}

	return ctrl.Result{}, nil
}

func (r *DatabaseAccountReconciler) stageDatabaseCreate(ctx context.Context, dbAccount *v1.DatabaseAccount) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	secret, err := r.getSecret(ctx, dbAccount)
	if err != nil {
		logger.Error(err, "unable to create/retrieve secret")
		return ctrl.Result{
			RequeueAfter: 5 * time.Second,
		}, err
	}

	if dbName, v, err := r.AccountServer.IsDatabase(ctx, dbAccount.Name); err != nil {
		r.Recorder.Event(dbAccount, "Warning", "DatabaseCreate", fmt.Sprintf("Failed to init check for create database: %s", err))

		return ctrl.Result{}, err
	} else if v {
		r.Recorder.Event(dbAccount, "Warning", "DatabaseCreate", "Database already exists")
		r.setSecretKV(secret, accountsvr.DatabaseKeyDatabase, dbName)
	} else {
		dbName, err := r.AccountServer.CreateDatabase(ctx, dbAccount.Name, dbAccount.Name)

		r.setSecretKV(secret, accountsvr.DatabaseKeyDatabase, dbName)

		if err != nil {
			r.Recorder.Event(dbAccount, "Warning", "DatabaseCreate", fmt.Sprintf("Failed to create database: %s", err))
			return ctrl.Result{}, err
		}
	}

	r.Recorder.Event(dbAccount, "Normal", "DatabaseCreate", "Database created")
	r.setSecretKV(secret, accountsvr.DatabaseKeyDSN, accountsvr.GenerateDSN(secret))

	if err := r.Update(ctx, secret); err != nil {
		logger.Error(err, "unable to update secret")

		return ctrl.Result{
			RequeueAfter: 5 * time.Second,
		}, err
	}

	dbAccount.Status.Stage = v1.ReadyStage
	dbAccount.Status.Ready = true
	if err := r.Status().Update(ctx, dbAccount); err != nil {
		logger.Error(err, "unable to update DatabaseAccount")

		return ctrl.Result{
			RequeueAfter: 5 * time.Second,
		}, err
	}
	r.Recorder.Event(dbAccount, "Normal", "Ready", "Ready to use")

	return ctrl.Result{}, nil
}

func (r *DatabaseAccountReconciler) stageReady(ctx context.Context, dbAccount *v1.DatabaseAccount) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	logger.Info("Record is marked as ready, nothing to do", "name", dbAccount.Name)

	return ctrl.Result{}, nil
}

func (r *DatabaseAccountReconciler) getSecret(ctx context.Context, dbAccount *v1.DatabaseAccount) (*corev1.Secret, error) {
	var secret corev1.Secret

	if err := r.Get(ctx, r.nameFromAccount(dbAccount), &secret); apierrors.IsNotFound(err) {
		return &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:        r.nameFromAccount(dbAccount).Name,
				Namespace:   dbAccount.Namespace,
				Annotations: dbAccount.Spec.SecretTemplate.Annotations,
				Labels:      dbAccount.Spec.SecretTemplate.Labels,
			},
			StringData: map[string]string{},
			Data:       map[string][]byte{},
			Type:       `kubeprj.github.io/database-account`,
		}, ErrNewSecret
	} else if err != nil {
		return &secret, err
	}

	return &secret, nil
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

func (r *DatabaseAccountReconciler) nameFromAccount(dbAccount *v1.DatabaseAccount) types.NamespacedName {
	if dbAccount.Spec.SecretName != "" {
		return types.NamespacedName{
			Namespace: dbAccount.GetNamespace(),
			Name:      dbAccount.Spec.SecretName,
		}
	}

	return types.NamespacedName{
		Namespace: dbAccount.GetNamespace(),
		Name:      dbAccount.GetName(),
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *DatabaseAccountReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kubeprjgithubiov1.DatabaseAccount{}).
		Complete(r)
}

func (r *DatabaseAccountReconciler) applySecretChanges(ctx context.Context, dbAccount *v1.DatabaseAccount, f func(secret *corev1.Secret) error) error {
	logger := log.FromContext(ctx)

	secret, err := r.getSecret(ctx, dbAccount)
	if errors.Is(err, ErrNewSecret) {
		r.AccountServer.CopyConfigToSecret(secret)
		if err := r.Create(ctx, secret); err != nil {
			logger.Error(err, "unable to create secret")

			return err
		}

		secret, err = r.getSecret(ctx, dbAccount)
		if err != nil {
			logger.Error(err, "unable to retrieve secret after creating")

			return err
		}
	} else if err != nil {
		return err
	}

	if err := f(secret); err != nil {
		return err
	}

	if err := r.Update(ctx, secret); err != nil {
		logger.Error(err, "unable to update secret")

		return err
	}

	return nil
}
